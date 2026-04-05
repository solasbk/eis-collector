"""tr1_sweep.py -- Systematic TR1 sweep across all LSE-listed companies.

Loads the full list of ~3,800 LSE companies and searches for TR1
(Notification of Major Holdings) filings for each one. Tracks progress
in the database so it can resume across runs.

Each run processes a batch of companies (default 200). Once all companies
are done, the sweep resets and starts again to catch new filings.
"""

import json
import os
import re
import threading
import time
from datetime import datetime, date
from pathlib import Path

import httpx
from bs4 import BeautifulSoup


# ── Config ────────────────────────────────────────────────────────
BATCH_SIZE = 200  # companies per run
PAGE_MAX_CHARS = 10000
MAX_PAGES_PER_COMPANY = 3  # max TR1 pages to fetch per company


# ── Scan state ────────────────────────────────────────────────────
_sweep_lock = threading.Lock()
_sweep_state = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "phase": "idle",
    "phase_detail": "",
    "companies_total": 0,
    "companies_done": 0,
    "companies_this_run": 0,
    "investors_found": 0,
    "investors_saved": 0,
    "investors_duplicate": 0,
    "error": None,
    "log": [],
}


def get_sweep_status():
    with _sweep_lock:
        return dict(_sweep_state)


def _sweep_update(**kwargs):
    with _sweep_lock:
        _sweep_state.update(kwargs)


def _sweep_log(msg):
    with _sweep_lock:
        _sweep_state["log"].append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
        if len(_sweep_state["log"]) > 200:
            _sweep_state["log"] = _sweep_state["log"][-200:]
    print(f"[tr1_sweep] {msg}")


# ── Load company list ─────────────────────────────────────────────

def _load_companies():
    """Load the LSE companies list from the bundled JSON file."""
    path = Path(__file__).parent / "lse_companies.json"
    if not path.exists():
        _sweep_log("lse_companies.json not found!")
        return []
    with open(path) as f:
        return json.load(f)


# ── Database: sweep progress tracking ─────────────────────────────

def _init_sweep_table():
    """Create the sweep progress table if it doesn't exist."""
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tr1_sweep_progress (
            id SERIAL PRIMARY KEY,
            company_name TEXT NOT NULL UNIQUE,
            ticker TEXT,
            last_searched TIMESTAMP,
            tr1_count INTEGER DEFAULT 0
        )
    """)
    db.commit()
    cur.close()
    db.close()


def _get_next_batch(companies, batch_size):
    """Get the next batch of companies that haven't been searched recently.
    
    Returns companies that either:
    - Have never been searched, or
    - Were last searched more than 7 days ago (to catch new filings)
    """
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    
    # Get all previously searched companies and their last search date
    cur.execute("SELECT company_name, last_searched FROM tr1_sweep_progress")
    searched = {row["company_name"]: row["last_searched"] for row in cur.fetchall()}
    cur.close()
    db.close()
    
    now = datetime.now()
    batch = []
    
    for company in companies:
        name = company["name"]
        last = searched.get(name)
        if last is None:
            # Never searched
            batch.append(company)
        elif (now - last).days >= 7:
            # Searched more than 7 days ago — rescan
            batch.append(company)
        
        if len(batch) >= batch_size:
            break
    
    return batch


def _mark_company_searched(company_name, ticker, tr1_count):
    """Record that a company has been searched."""
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO tr1_sweep_progress (company_name, ticker, last_searched, tr1_count)
        VALUES (%s, %s, NOW(), %s)
        ON CONFLICT (company_name) 
        DO UPDATE SET last_searched = NOW(), tr1_count = EXCLUDED.tr1_count
    """, (company_name, ticker, tr1_count))
    db.commit()
    cur.close()
    db.close()


def _get_sweep_progress():
    """Get overall sweep progress stats."""
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) as c FROM tr1_sweep_progress")
    total_done = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM tr1_sweep_progress WHERE last_searched > NOW() - INTERVAL '7 days'")
    recent = cur.fetchone()["c"]
    cur.close()
    db.close()
    return {"total_searched": total_done, "searched_last_7_days": recent}


# ── Search and extract ────────────────────────────────────────────

def _search_company_tr1(company_name, serper_key):
    """Search for TR1 filings for a specific company."""
    results = []
    seen_urls = set()
    
    queries = [
        f'site:investegate.co.uk "TR-1" "{company_name}"',
        f'"notification of major holdings" "{company_name}"',
    ]
    
    for query in queries:
        try:
            resp = httpx.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": serper_key, "Content-Type": "application/json"},
                json={"q": query, "num": 10},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("organic", []):
                    url = item.get("link", "")
                    title = item.get("title", "").lower()
                    # Only keep results that mention TR1 or major holdings
                    if url not in seen_urls and (
                        "tr-1" in title or "tr1" in title or 
                        "major holding" in title or "notification" in title or
                        "investegate" in url.lower()
                    ):
                        seen_urls.add(url)
                        results.append({
                            "title": item.get("title", ""),
                            "url": url,
                            "snippet": item.get("snippet", ""),
                        })
            elif resp.status_code == 429:
                _sweep_log("Serper rate limited. Waiting 30s...")
                time.sleep(30)
        except Exception as e:
            _sweep_log(f"Serper error for {company_name}: {e}")
        time.sleep(0.3)
    
    return results[:MAX_PAGES_PER_COMPANY]


def _fetch_page_text(url):
    """Fetch and extract text from a page."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
        }
        with httpx.Client(timeout=15, follow_redirects=True) as client:
            resp = client.get(url, headers=headers)
            if resp.status_code != 200:
                return ""
            soup = BeautifulSoup(resp.text, "html.parser")
            for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
                tag.decompose()
            text = soup.get_text(separator="\n", strip=True)
            return text[:PAGE_MAX_CHARS]
    except Exception:
        return ""


def _extract_with_llm(page_text, url, title, company_name):
    """Extract investor data from a TR1 filing page."""
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

    prompt = f"""Analyze this TR1 (Notification of Major Holdings) announcement for {company_name} and extract ALL persons and entities named.

Page title: {title}
Page URL: {url}

Page content:
{page_text}

Extract for each person/entity:
- name: Full name of person or entity
- issuer: Company whose shares are held (should be {company_name} or similar)
- holding_pct: Percentage of voting rights
- num_shares: Number of shares
- notification_date: Date of notification
- reason: Brief reason (acquisition, disposal, etc.)
- entity_type: "Individual" or "Organisation"

Return ONLY a JSON array. If no data found, return: []"""

    providers = []
    if gemini_key:
        providers.append(("gemini", gemini_key))
    if anthropic_key:
        providers.append(("anthropic", anthropic_key))

    for prov_name, prov_key in providers:
        try:
            if prov_name == "gemini":
                resp = httpx.post(
                    "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent",
                    headers={"x-goog-api-key": prov_key, "Content-Type": "application/json"},
                    json={"contents": [{"parts": [{"text": prompt}]}]},
                    timeout=60,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                    return _parse_response(text)
            else:
                import anthropic
                client = anthropic.Anthropic(api_key=prov_key)
                msg = client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=2000,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = msg.content[0].text if msg.content else ""
                return _parse_response(text)
        except Exception as e:
            _sweep_log(f"{prov_name} error: {e}")
    
    return []


def _parse_response(text):
    """Parse JSON array from LLM response."""
    text = text.strip()
    match = re.search(r'\[.*\]', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        pass
    return []


def _save_investors(investors):
    """Save investors to the database."""
    from api_server import get_db
    db = get_db()
    cur = db.cursor()

    inserted = 0
    duplicated = 0

    for inv in investors:
        cur.execute(
            "SELECT id FROM investors WHERE name = %s AND eis_company = %s",
            (inv.get("name", ""), inv.get("eis_company", ""))
        )
        if cur.fetchone():
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


# ── Main entry point ──────────────────────────────────────────────

def run_tr1_sweep():
    """Run a batch of the TR1 company-by-company sweep."""
    with _sweep_lock:
        if _sweep_state["running"]:
            return False
        _sweep_state.update({
            "running": True,
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "phase": "loading",
            "phase_detail": "Loading company list...",
            "companies_total": 0,
            "companies_done": 0,
            "companies_this_run": 0,
            "investors_found": 0,
            "investors_saved": 0,
            "investors_duplicate": 0,
            "error": None,
            "log": [],
        })

    def _run():
        try:
            serper_key = os.environ.get("SERPER_API_KEY", "")
            if not serper_key:
                _sweep_log("SERPER_API_KEY not set.")
                _sweep_update(phase="error", phase_detail="SERPER_API_KEY not set.", 
                             error="No API key", running=False, finished_at=datetime.now().isoformat())
                return

            # Init tracking table
            _init_sweep_table()

            # Load companies
            companies = _load_companies()
            if not companies:
                _sweep_update(phase="error", phase_detail="No companies list found.",
                             error="Missing lse_companies.json", running=False, 
                             finished_at=datetime.now().isoformat())
                return

            _sweep_update(companies_total=len(companies))
            _sweep_log(f"Loaded {len(companies)} LSE companies")

            # Get progress
            progress = _get_sweep_progress()
            _sweep_log(f"Previously searched: {progress['total_searched']} total, {progress['searched_last_7_days']} in last 7 days")

            # Get next batch
            batch = _get_next_batch(companies, BATCH_SIZE)
            if not batch:
                _sweep_log("All companies searched within last 7 days. Sweep complete.")
                _sweep_update(
                    phase="done",
                    phase_detail=f"All {len(companies)} companies searched. Sweep will restart after 7 days.",
                    running=False, finished_at=datetime.now().isoformat(),
                    companies_done=progress["total_searched"],
                )
                return

            _sweep_log(f"Processing batch of {len(batch)} companies")
            _sweep_update(phase="searching", phase_detail=f"Processing {len(batch)} companies...")

            today_str = date.today().isoformat()
            total_inserted = 0
            total_duplicated = 0
            total_found = 0

            for i, company in enumerate(batch):
                name = company["name"]
                ticker = company.get("ticker", "")

                _sweep_update(
                    phase="extracting",
                    phase_detail=f"({i+1}/{len(batch)}) Searching: {name}",
                    companies_this_run=i + 1,
                )

                # Search for TR1 filings
                tr1_pages = _search_company_tr1(name, serper_key)
                tr1_count = 0

                if tr1_pages:
                    for page in tr1_pages:
                        page_text = _fetch_page_text(page["url"])
                        if not page_text or len(page_text.strip()) < 200:
                            continue

                        extracted = _extract_with_llm(page_text, page["url"], page["title"], name)
                        if extracted:
                            investors = []
                            for item in extracted:
                                entity_type = item.get("entity_type", "Individual")
                                inv_name = item.get("name", "").strip()
                                issuer = item.get("issuer", "").strip() or name
                                if not inv_name:
                                    continue
                                investors.append({
                                    "name": inv_name,
                                    "role": f"Shareholder ({item.get('holding_pct', 'N/A')})",
                                    "company": entity_type,
                                    "eis_company": issuer,
                                    "sector": "Listed Company",
                                    "amount": f"{item.get('num_shares', 'N/A')} shares",
                                    "source_url": page["url"],
                                    "source_type": "Filing",
                                    "source_name": "LSE TR1 Filing",
                                    "context_quote": f"TR1 sweep ({entity_type}): {item.get('reason', 'Major holding')}. {item.get('holding_pct', 'N/A')} ({item.get('num_shares', 'N/A')} shares). {item.get('notification_date', '')}",
                                    "linkedin_url": None,
                                    "date_found": today_str,
                                })

                            if investors:
                                ins, dup = _save_investors(investors)
                                total_inserted += ins
                                total_duplicated += dup
                                total_found += len(investors)
                                tr1_count += len(investors)

                        time.sleep(0.5)

                # Mark as searched
                _mark_company_searched(name, ticker, tr1_count)
                
                if tr1_count > 0:
                    _sweep_log(f"{name}: {tr1_count} entities found")

                _sweep_update(
                    investors_found=total_found,
                    investors_saved=total_inserted,
                    investors_duplicate=total_duplicated,
                )

            # Done
            progress_after = _get_sweep_progress()
            _sweep_log(f"Batch complete. {total_inserted} new, {total_duplicated} duplicates. {progress_after['total_searched']}/{len(companies)} companies searched.")

            _sweep_update(
                phase="done",
                phase_detail=f"Batch done: {total_inserted} new investors from {len(batch)} companies. Overall: {progress_after['total_searched']}/{len(companies)} companies searched.",
                running=False,
                finished_at=datetime.now().isoformat(),
                companies_done=progress_after["total_searched"],
            )

        except Exception as e:
            _sweep_log(f"Sweep error: {e}")
            _sweep_update(
                phase="error", phase_detail=str(e), error=str(e),
                running=False, finished_at=datetime.now().isoformat(),
            )

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return True
