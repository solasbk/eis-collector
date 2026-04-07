"""ch_scanner.py -- Companies House PSC scanner for EIS investors.

Searches Companies House for known EIS-qualifying companies,
then pulls Persons with Significant Control (PSC) data to find
individual shareholders/investors.
"""

import json
import os
import threading
import time
from datetime import datetime, date, timedelta
from typing import Optional

import httpx

CH_API_BASE = "https://api.company-information.service.gov.uk"

# ── Scan state (separate from web scanner) ────────────────────────
_ch_lock = threading.Lock()
_ch_state = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "phase": "idle",
    "phase_detail": "",
    "companies_searched": 0,
    "companies_found": 0,
    "investors_found": 0,
    "investors_saved": 0,
    "investors_duplicate": 0,
    "error": None,
    "log": [],
}


def get_ch_scan_status():
    with _ch_lock:
        return dict(_ch_state)


def _ch_update(**kwargs):
    with _ch_lock:
        _ch_state.update(kwargs)


def _ch_log(msg):
    with _ch_lock:
        _ch_state["log"].append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
        if len(_ch_state["log"]) > 150:
            _ch_state["log"] = _ch_state["log"][-150:]
    print(f"[ch_scanner] {msg}")


# ── SIC codes likely to be EIS-qualifying ─────────────────────────
# Information & Communication, Professional/Scientific/Technical,
# Manufacturing (selected), other tech-adjacent codes
EIS_SIC_PREFIXES = [
    "58", "59", "60", "61", "62", "63",  # Information & Communication
    "69", "70", "71", "72", "73", "74",  # Professional, Scientific, Technical
    "26", "27", "28",                      # Electronics, Electrical, Machinery manufacturing
    "20", "21",                            # Chemicals, Pharmaceuticals
    "75",                                  # Veterinary
    "86",                                  # Human health (medtech)
]


# ── Seed list of known EIS/SEIS company search terms ──────────────
# These are used to find companies on Companies House. The search
# also pulls company names from the existing investor database.
SEED_COMPANY_SEARCHES = [
    # Well-known EIS-backed companies
    "Seedrs", "Crowdcube", "SyndicateRoom", "Monzo", "Revolut",
    "Brewdog", "Bulb Energy", "Octopus Energy", "Darktrace",
    "Graphcore", "Babylon Health", "Cazoo", "Checkout.com",
    "Wise", "Deliveroo", "Improbable",
    # EIS fund managers and platforms
    "Oxford Capital", "Calculus Capital", "Puma Investments",
    "Triple Point", "Deepbridge Capital", "Par Equity",
    "Mercia Asset Management", "Foresight Group",
    # Recent UK startup terms to find young EIS-qualifying companies
    "AI Limited", "Tech Limited", "Labs Limited", "Health Limited",
    "Bio Limited", "Digital Limited", "Software Limited",
    "Ventures Limited", "Solutions Limited", "Therapeutics Limited",
    "Robotics Limited", "Analytics Limited", "Fintech Limited",
]


def _get_api_key():
    key = os.environ.get("COMPANIES_HOUSE_API_KEY", "")
    if not key:
        return None
    return key


def _ch_api_get(path, api_key, params=None):
    """Make an authenticated GET request to Companies House API."""
    url = f"{CH_API_BASE}{path}"
    resp = httpx.get(
        url,
        auth=(api_key, ""),  # CH API uses basic auth with key as username
        params=params,
        timeout=15,
    )
    return resp


# ── Search for companies ──────────────────────────────────────────

def _search_companies(api_key, query, max_results=20):
    """Search Companies House for companies matching a query."""
    results = []
    try:
        resp = _ch_api_get(
            "/search/companies",
            api_key,
            params={"q": query, "items_per_page": min(max_results, 20)},
        )
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get("items", []):
                # Filter to active, private limited companies
                status = item.get("company_status", "")
                company_type = item.get("company_type", "")
                if status == "active" and company_type in ("ltd", "private-limited-guarant-nsc"):
                    results.append({
                        "company_number": item.get("company_number"),
                        "company_name": item.get("title", ""),
                        "date_of_creation": item.get("date_of_creation", ""),
                        "address_snippet": item.get("address_snippet", ""),
                        "sic_codes": item.get("sic_codes", []),
                    })
        elif resp.status_code == 429:
            _ch_log("Rate limited by Companies House. Waiting 60s...")
            time.sleep(60)
        else:
            _ch_log(f"Search returned {resp.status_code} for '{query}'")
    except Exception as e:
        _ch_log(f"Search error for '{query}': {e}")
    return results


def _is_likely_eis_company(company):
    """Check if a company is likely EIS-qualifying based on available data."""
    sic_codes = company.get("sic_codes", [])
    if sic_codes:
        for code in sic_codes:
            if any(code.startswith(prefix) for prefix in EIS_SIC_PREFIXES):
                return True
    # If no SIC codes available, include it anyway (we'll filter by PSC results)
    if not sic_codes:
        return True
    return False


# ── Get PSC data for a company ────────────────────────────────────

def _get_psc_individuals(api_key, company_number):
    """Get individual Persons with Significant Control for a company."""
    individuals = []
    try:
        resp = _ch_api_get(
            f"/company/{company_number}/persons-with-significant-control",
            api_key,
        )
        if resp.status_code == 200:
            data = resp.json()
            for item in data.get("items", []):
                kind = item.get("kind", "")
                if kind == "individual-person-with-significant-control":
                    name = item.get("name", "")
                    # Skip if no name or if it's a corporate entity
                    if not name or name.upper() == name:  # All-caps often = corporate
                        continue
                    individuals.append({
                        "name": name,
                        "nationality": item.get("nationality", ""),
                        "country_of_residence": item.get("country_of_residence", ""),
                        "natures_of_control": item.get("natures_of_control", []),
                        "notified_on": item.get("notified_on", ""),
                        "ceased_on": item.get("ceased_on"),
                    })
        elif resp.status_code == 429:
            _ch_log("Rate limited. Waiting 60s...")
            time.sleep(60)
    except Exception as e:
        _ch_log(f"PSC error for {company_number}: {e}")
    return individuals


def _get_company_profile(api_key, company_number):
    """Get basic company profile to determine SIC codes and other details."""
    try:
        resp = _ch_api_get(f"/company/{company_number}", api_key)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


# ── Get company names from existing database ──────────────────────

def _get_eis_companies_from_db():
    """Pull unique EIS company names from the existing investor database."""
    try:
        from api_server import get_db
        db = get_db()
        cur = db.cursor()
        cur.execute(
            "SELECT DISTINCT eis_company FROM investors WHERE eis_company IS NOT NULL AND eis_company != ''"
        )
        rows = cur.fetchall()
        cur.close()
        db.close()
        return [row["eis_company"] for row in rows]
    except Exception:
        return []


# ── Save investors to database ────────────────────────────────────

def _save_ch_investors(investors):
    """Save Companies House investors to database."""
    from api_server import get_db
    db = get_db()
    cur = db.cursor()

    inserted = 0
    duplicated = 0

    for inv in investors:
        cur.execute(
            "SELECT id FROM investors WHERE name = %s AND eis_company = %s",
            [inv.get("name", ""), inv.get("eis_company", "")]
        )
        existing = cur.fetchone()

        if existing:
            duplicated += 1
        else:
            cur.execute("""
                INSERT INTO investors (name, role, company, eis_company, sector, amount,
                source_url, source_type, source_name, context_quote, linkedin_url, date_found)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                inv.get("name"), inv.get("role"), inv.get("company"),
                inv.get("eis_company"), inv.get("sector"), inv.get("amount"),
                inv.get("source_url"), inv.get("source_type"), inv.get("source_name"),
                inv.get("context_quote"), inv.get("linkedin_url"), inv.get("date_found"),
            ))
            inserted += 1

    db.commit()
    cur.close()
    db.close()
    return inserted, duplicated


# ── Main scan entry point ─────────────────────────────────────────

def _get_recent_eis_companies(since_date):
    """Get EIS company names added to DB since a given date."""
    from api_server import get_db
    try:
        db = get_db()
        cur = db.cursor()
        cur.execute(
            "SELECT DISTINCT eis_company FROM investors WHERE eis_company IS NOT NULL AND eis_company != '' AND date_found >= %s",
            (since_date,)
        )
        result = [row["eis_company"] for row in cur.fetchall()]
        cur.close()
        db.close()
        return result
    except Exception:
        return []


def run_ch_scan(recent_only=False):
    """Execute a Companies House PSC scan.
    
    Args:
        recent_only: If True, only search companies added since last CH scan.
    """
    with _ch_lock:
        if _ch_state["running"]:
            return False
        _ch_state.update({
            "running": True,
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "phase": "searching",
            "phase_detail": "Starting Companies House scan...",
            "companies_searched": 0,
            "companies_found": 0,
            "investors_found": 0,
            "investors_saved": 0,
            "investors_duplicate": 0,
            "error": None,
            "log": [],
        })

    def _run():
        try:
            api_key = _get_api_key()
            if not api_key:
                _ch_log("COMPANIES_HOUSE_API_KEY not set.")
                _ch_update(
                    phase="error",
                    phase_detail="COMPANIES_HOUSE_API_KEY not configured in Render.",
                    error="No API key",
                    running=False,
                    finished_at=datetime.now().isoformat(),
                )
                return

            _ch_log("Companies House API key found.")
            today = date.today().isoformat()

            # Step 1: Build search list
            if recent_only:
                # Only search companies added since last CH scan
                _ch_update(phase="searching", phase_detail="Building recent company list...")
                try:
                    from api_server import _get_last_scan_dates
                    scan_dates = _get_last_scan_dates()
                    last_ch = scan_dates.get("ch", {}).get("last_run", "")
                    if last_ch:
                        since = last_ch[:10]  # YYYY-MM-DD
                    else:
                        since = (date.today() - timedelta(days=7)).isoformat()
                except Exception:
                    since = (date.today() - timedelta(days=7)).isoformat()
                
                db_companies = _get_recent_eis_companies(since)
                _ch_log(f"Recent mode: {len(db_companies)} new companies since {since}")
                all_searches = list(set(db_companies))
            else:
                _ch_update(phase="searching", phase_detail="Building company search list...")
                db_companies = _get_eis_companies_from_db()
                _ch_log(f"Found {len(db_companies)} company names from existing database")
                all_searches = list(set(SEED_COMPANY_SEARCHES + db_companies))
            _ch_log(f"Total search terms: {len(all_searches)}")

            # Step 2: Search Companies House for each company
            seen_company_numbers = set()
            companies_to_scan = []
            request_count = 0

            for i, search_term in enumerate(all_searches):
                _ch_update(
                    phase="searching",
                    phase_detail=f"Searching Companies House ({i+1}/{len(all_searches)}): {search_term[:40]}...",
                    companies_searched=i + 1,
                )

                results = _search_companies(api_key, search_term, max_results=5)
                request_count += 1

                for co in results:
                    cn = co["company_number"]
                    if cn not in seen_company_numbers:
                        seen_company_numbers.add(cn)
                        companies_to_scan.append(co)

                # Rate limiting: 600 requests per 5 min = ~2 per second
                if request_count % 100 == 0:
                    _ch_log(f"Pausing after {request_count} requests (rate limit management)...")
                    time.sleep(10)
                else:
                    time.sleep(0.5)

            _ch_log(f"Found {len(companies_to_scan)} unique companies to scan for PSCs")
            _ch_update(companies_found=len(companies_to_scan))

            # Step 3: Pull PSC data for each company
            all_investors = []
            companies_with_pscs = 0

            for i, company in enumerate(companies_to_scan):
                cn = company["company_number"]
                name = company["company_name"]

                _ch_update(
                    phase="extracting",
                    phase_detail=f"Pulling PSC data ({i+1}/{len(companies_to_scan)}): {name[:40]}...",
                )

                individuals = _get_psc_individuals(api_key, cn)
                request_count += 1

                if individuals:
                    companies_with_pscs += 1
                    for psc in individuals:
                        # Skip ceased PSCs
                        if psc.get("ceased_on"):
                            continue

                        # Build investor record
                        natures = psc.get("natures_of_control", [])
                        control_desc = "; ".join(natures) if natures else "Shareholder"

                        investor = {
                            "name": psc["name"],
                            "role": "Shareholder / PSC",
                            "company": "Independent",
                            "eis_company": name,
                            "sector": _guess_sector(company.get("sic_codes", [])),
                            "amount": "Undisclosed",
                            "source_url": f"https://find-and-update.company-information.service.gov.uk/company/{cn}/persons-with-significant-control",
                            "source_type": "Filing",
                            "source_name": "Companies House",
                            "context_quote": f"PSC of {name} ({cn}). Control: {control_desc}",
                            "linkedin_url": None,
                            "date_found": today,
                        }
                        all_investors.append(investor)

                _ch_update(investors_found=len(all_investors))

                # Rate limiting
                if request_count % 100 == 0:
                    _ch_log(f"Pausing after {request_count} API calls...")
                    time.sleep(10)
                else:
                    time.sleep(0.5)

            _ch_log(f"Found {len(all_investors)} individual PSCs across {companies_with_pscs} companies")

            if not all_investors:
                _ch_update(
                    phase="done",
                    phase_detail=f"Scanned {len(companies_to_scan)} companies. No new individual PSCs found.",
                    running=False,
                    finished_at=datetime.now().isoformat(),
                )
                return

            # Step 4: Save to database
            _ch_update(
                phase="saving",
                phase_detail=f"Saving {len(all_investors)} investor(s) to database...",
            )
            inserted, duplicated = _save_ch_investors(all_investors)
            _ch_log(f"Saved: {inserted} new, {duplicated} duplicates")

            _ch_update(
                phase="done",
                phase_detail=f"Done. {inserted} new investor(s), {duplicated} duplicate(s). Scanned {len(companies_to_scan)} companies.",
                running=False,
                finished_at=datetime.now().isoformat(),
                investors_saved=inserted,
                investors_duplicate=duplicated,
            )

        except Exception as e:
            _ch_log(f"Scan error: {e}")
            _ch_update(
                phase="error",
                phase_detail=str(e),
                error=str(e),
                running=False,
                finished_at=datetime.now().isoformat(),
            )

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return True


def _guess_sector(sic_codes):
    """Map SIC codes to a rough sector label."""
    if not sic_codes:
        return "Various"
    code = sic_codes[0] if isinstance(sic_codes, list) else str(sic_codes)
    code = str(code)[:2]
    mapping = {
        "58": "Publishing / Software",
        "59": "Media / Film",
        "60": "Broadcasting",
        "61": "Telecommunications",
        "62": "Software / IT",
        "63": "Information Services / Data",
        "64": "Financial Services",
        "65": "Insurance",
        "66": "Financial Services",
        "69": "Legal / Accounting",
        "70": "Management Consultancy",
        "71": "Engineering / Architecture",
        "72": "R&D / Science",
        "73": "Advertising / Marketing",
        "74": "Professional Services",
        "20": "Chemicals",
        "21": "Pharmaceuticals",
        "26": "Electronics",
        "27": "Electrical Equipment",
        "28": "Machinery",
        "46": "Wholesale",
        "47": "Retail",
        "75": "Veterinary",
        "86": "Healthcare",
        "85": "Education",
        "10": "Food Manufacturing",
        "56": "Food & Beverage",
    }
    return mapping.get(code, "Various")
