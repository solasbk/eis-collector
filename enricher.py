"""enricher.py -- LinkedIn enrichment via FullEnrich API.

Takes individual investors (not organisations), looks up their LinkedIn
profile using FullEnrich's People Search API, and updates the database.

Uses: first name + last name + issuer company + UK location.
"""

import os
import re
import threading
import time
from datetime import datetime

import httpx

FULLENRICH_API = "https://app.fullenrich.com/api/v2/people/search"

# ── Scan state ────────────────────────────────────────────────────
_enrich_lock = threading.Lock()
_enrich_state = {
    "running": False,
    "stop_requested": False,
    "started_at": None,
    "finished_at": None,
    "phase": "idle",
    "phase_detail": "",
    "total": 0,
    "checked": 0,
    "found": 0,
    "not_found": 0,
    "already_have": 0,
    "error": None,
    "log": [],
}


def get_enrich_status():
    with _enrich_lock:
        return dict(_enrich_state)


def stop_enrichment():
    with _enrich_lock:
        if _enrich_state["running"]:
            _enrich_state["stop_requested"] = True
            return True
    return False


def _enrich_update(**kwargs):
    with _enrich_lock:
        _enrich_state.update(kwargs)


def _enrich_log(msg):
    with _enrich_lock:
        _enrich_state["log"].append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
        if len(_enrich_state["log"]) > 200:
            _enrich_state["log"] = _enrich_state["log"][-200:]
    print(f"[enricher] {msg}")


def _parse_name(full_name):
    """Split a full name into first and last name."""
    # Remove titles
    name = re.sub(r'^(Mr\.?|Mrs\.?|Ms\.?|Miss|Dr\.?|Prof\.?|Sir|Lord|Lady|Dame|Baron|Baroness)\s+', '', full_name.strip(), flags=re.IGNORECASE)
    parts = name.strip().split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    elif len(parts) == 1:
        return parts[0], ""
    return "", ""


def _search_fullenrich(api_key, first_name, last_name, company_name):
    """Search FullEnrich for a person's LinkedIn profile.
    Returns linkedin_url or None.
    """
    try:
        payload = {
            "limit": 1,
            "person_names": [{"value": f"{first_name} {last_name}", "exact_match": False, "exclude": False}],
            "person_locations": [
                {"value": "United Kingdom", "exact_match": False, "exclude": False},
                {"value": "Ireland", "exact_match": False, "exclude": False},
            ],
        }
        
        resp = httpx.post(
            FULLENRICH_API,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=15,
        )
        
        if resp.status_code == 200:
            data = resp.json()
            people = data.get("people", [])
            if people:
                person = people[0]
                linkedin = person.get("social_profiles", {}).get("linkedin", {})
                url = linkedin.get("url", "")
                if url:
                    return url
        elif resp.status_code == 429:
            _enrich_log("FullEnrich rate limited. Waiting 30s...")
            time.sleep(30)
        else:
            _enrich_log(f"FullEnrich returned {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        _enrich_log(f"FullEnrich error: {e}")
    
    return None


def _init_enrich_table():
    """Create table to track which investors have been checked."""
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS enrich_checked (
            investor_id INTEGER PRIMARY KEY,
            checked_at TIMESTAMP DEFAULT NOW(),
            linkedin_found BOOLEAN DEFAULT FALSE
        )
    """)
    db.commit()
    cur.close()
    db.close()


def _mark_checked(investor_id, linkedin_found):
    """Mark an investor as checked by FullEnrich."""
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO enrich_checked (investor_id, checked_at, linkedin_found)
        VALUES (%s, NOW(), %s)
        ON CONFLICT (investor_id) DO UPDATE SET checked_at = NOW(), linkedin_found = EXCLUDED.linkedin_found
    """, (investor_id, linkedin_found))
    db.commit()
    cur.close()
    db.close()


def _get_individuals_without_linkedin(limit=500):
    """Get individuals not yet checked by FullEnrich.
    Skips: organisations, those with LinkedIn already, and those already checked."""
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        SELECT i.id, i.name, i.company, i.eis_company
        FROM investors i
        LEFT JOIN enrich_checked ec ON i.id = ec.investor_id
        WHERE (i.linkedin_url IS NULL OR i.linkedin_url = '')
        AND (i.company != 'Organisation' OR i.company IS NULL)
        AND i.name IS NOT NULL AND i.name != ''
        AND ec.investor_id IS NULL
        ORDER BY i.date_found DESC
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    cur.close()
    db.close()
    return [dict(r) for r in rows]


def _update_linkedin(investor_id, linkedin_url):
    """Update the LinkedIn URL for an investor."""
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute(
        "UPDATE investors SET linkedin_url = %s WHERE id = %s",
        (linkedin_url, investor_id)
    )
    db.commit()
    cur.close()
    db.close()


# ── Main entry point ──────────────────────────────────────────────

def run_enrichment():
    """Enrich individuals with LinkedIn profiles via FullEnrich."""
    with _enrich_lock:
        if _enrich_state["running"]:
            return False
        _enrich_state["stop_requested"] = False
        _enrich_state.update({
            "running": True,
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "phase": "loading",
            "phase_detail": "Loading investors without LinkedIn...",
            "total": 0, "checked": 0, "found": 0,
            "not_found": 0, "already_have": 0,
            "error": None, "log": [],
        })

    def _run():
        try:
            _init_enrich_table()

            api_key = os.environ.get("FULLENRICH_API_KEY", "")
            if not api_key:
                _enrich_log("FULLENRICH_API_KEY not set.")
                _enrich_update(phase="error", phase_detail="FULLENRICH_API_KEY not configured in Render.",
                              error="No API key", running=False, finished_at=datetime.now().isoformat())
                return

            # Get individuals without LinkedIn
            investors = _get_individuals_without_linkedin(limit=100)
            if not investors:
                _enrich_update(
                    phase="done", phase_detail="All individuals have been checked with FullEnrich. New investors will be checked on next run.",
                    running=False, finished_at=datetime.now().isoformat(),
                )
                return

            _enrich_log(f"Found {len(investors)} individuals without LinkedIn profiles")
            _enrich_update(total=len(investors), phase="enriching")

            found = 0
            not_found = 0

            for i, inv in enumerate(investors):
                # Check for stop
                with _enrich_lock:
                    if _enrich_state["stop_requested"]:
                        _enrich_log("Stop requested.")
                        break

                name = inv["name"]
                company = inv.get("eis_company") or inv.get("company") or ""
                first, last = _parse_name(name)

                if not first or not last:
                    not_found += 1
                    continue

                _enrich_update(
                    phase_detail=f"Enriching ({i+1}/{len(investors)}): {first} {last} — {company[:30]} | Found: {found}",
                    checked=i + 1,
                )

                linkedin_url = _search_fullenrich(api_key, first, last, company)

                if linkedin_url:
                    _update_linkedin(inv["id"], linkedin_url)
                    _mark_checked(inv["id"], True)
                    found += 1
                    _enrich_log(f"Found: {first} {last} → {linkedin_url}")
                else:
                    _mark_checked(inv["id"], False)
                    not_found += 1

                _enrich_update(found=found, not_found=not_found)
                time.sleep(0.5)  # Rate limiting

            _enrich_log(f"Done. Found {found} LinkedIn profiles, {not_found} not found.")
            _enrich_update(
                phase="done",
                phase_detail=f"Done: {found} LinkedIn profiles found, {not_found} not found (out of {len(investors)} individuals).",
                running=False,
                finished_at=datetime.now().isoformat(),
            )

        except Exception as e:
            _enrich_log(f"Error: {e}")
            _enrich_update(phase="error", phase_detail=str(e), error=str(e),
                          running=False, finished_at=datetime.now().isoformat())

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return True
