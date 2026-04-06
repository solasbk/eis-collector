"""tr1_direct.py -- TR1 scan for LSE companies filtered by market cap.

Instead of scanning every Investegate ID sequentially, this module:
1. Loads the LSE companies list
2. Filters to companies under the market cap threshold (via yfinance)
3. Searches Investegate for TR1 filings per qualifying company
4. Extracts investor data using Gemini

Progress is tracked per-company in the database.
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
COMPANIES_PER_BATCH = 50  # companies to process before saving progress
MAX_TR1_PER_COMPANY = 5   # max TR1 pages to fetch per company
PAGE_MAX_CHARS = 10000

# ── Scan state ────────────────────────────────────────────────────
_direct_lock = threading.Lock()
_direct_state = {
    "running": False,
    "stop_requested": False,
    "started_at": None,
    "finished_at": None,
    "phase": "idle",
    "phase_detail": "",
    "companies_total": 0,
    "companies_checked": 0,
    "companies_qualified": 0,
    "companies_with_tr1": 0,
    "investors_found": 0,
    "investors_saved": 0,
    "investors_duplicate": 0,
    "skipped_large": 0,
    "skipped_no_data": 0,
    "max_market_cap": 0,
    "progress_pct": 0,
    "error": None,
    "log": [],
}


def get_direct_status():
    with _direct_lock:
        return dict(_direct_state)


def stop_direct_scan():
    with _direct_lock:
        if _direct_state["running"]:
            _direct_state["stop_requested"] = True
            return True
    return False


def _direct_update(**kwargs):
    with _direct_lock:
        _direct_state.update(kwargs)


def _direct_log(msg):
    with _direct_lock:
        _direct_state["log"].append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
        if len(_direct_state["log"]) > 200:
            _direct_state["log"] = _direct_state["log"][-200:]
    print(f"[tr1_direct] {msg}")


# ── Load company list ─────────────────────────────────────────────

def _load_companies():
    path = Path(__file__).parent / "lse_companies.json"
    if not path.exists():
        return []
    with open(path) as f:
        return json.load(f)


# ── Database: progress tracking ───────────────────────────────────

def _init_tables():
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tr1_company_progress (
            id SERIAL PRIMARY KEY,
            company_name TEXT NOT NULL UNIQUE,
            ticker TEXT,
            market_cap_m INTEGER DEFAULT 0,
            last_searched TIMESTAMP,
            tr1_investors_found INTEGER DEFAULT 0
        )
    """)
    db.commit()
    cur.close()
    db.close()


def _get_searched_companies():
    """Get set of company names searched in the last 7 days."""
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        SELECT company_name FROM tr1_company_progress 
        WHERE last_searched > NOW() - INTERVAL '7 days'
    """)
    result = set(row["company_name"] for row in cur.fetchall())
    cur.close()
    db.close()
    return result


def _mark_searched(company_name, ticker, market_cap_m, tr1_count):
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO tr1_company_progress (company_name, ticker, market_cap_m, last_searched, tr1_investors_found)
        VALUES (%s, %s, %s, NOW(), %s)
        ON CONFLICT (company_name)
        DO UPDATE SET ticker = EXCLUDED.ticker, market_cap_m = EXCLUDED.market_cap_m,
                      last_searched = NOW(), tr1_investors_found = EXCLUDED.tr1_investors_found
    """, (company_name, ticker, market_cap_m, tr1_count))
    db.commit()
    cur.close()
    db.close()


def _get_progress_stats():
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) as c FROM tr1_company_progress")
    total = cur.fetchone()["c"]
    cur.execute("SELECT COUNT(*) as c FROM tr1_company_progress WHERE last_searched > NOW() - INTERVAL '7 days'")
    recent = cur.fetchone()["c"]
    cur.close()
    db.close()
    return total, recent


# ── Market cap lookup ─────────────────────────────────────────────

_mcap_cache = {}

def _get_market_cap(ticker, company_name):
    """Look up market cap in £ millions. Returns (market_cap_m, found)."""
    key = ticker.upper()
    if key in _mcap_cache:
        return _mcap_cache[key]

    try:
        import yfinance as yf

        # Try direct ticker first
        symbol = f"{ticker}.L"
        t = yf.Ticker(symbol)
        info = t.info
        mcap = info.get("marketCap", 0)
        if mcap and mcap > 0:
            mcap_m = int(mcap / 1e6)
            _mcap_cache[key] = (mcap_m, True)
            return (mcap_m, True)

        # Try search by name
        search = yf.Search(company_name, max_results=3)
        if search.quotes:
            for q in search.quotes:
                sym = q.get("symbol", "")
                if sym.endswith(".L"):
                    t2 = yf.Ticker(sym)
                    mcap2 = t2.info.get("marketCap", 0)
                    if mcap2 and mcap2 > 0:
                        mcap_m = int(mcap2 / 1e6)
                        _mcap_cache[key] = (mcap_m, True)
                        return (mcap_m, True)

        _mcap_cache[key] = (0, False)
        return (0, False)
    except Exception:
        _mcap_cache[key] = (0, False)
        return (0, False)


# ── TR1 search per company ────────────────────────────────────────

def _search_company_tr1(company_name, serper_key):
    """Search Investegate for TR1 filings for a specific company."""
    results = []
    seen = set()

    queries = [
        f'site:investegate.co.uk "TR-1" "{company_name}"',
        f'site:investegate.co.uk "Holding(s) in Company" "{company_name}"',
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
                for item in resp.json().get("organic", []):
                    url = item.get("link", "")
                    title = item.get("title", "").lower()
                    if url not in seen and ("holding" in title or "tr-1" in title or "tr1" in title or "notification" in title or "investegate" in url.lower()):
                        seen.add(url)
                        results.append({
                            "title": item.get("title", ""),
                            "url": url,
                            "snippet": item.get("snippet", ""),
                        })
            elif resp.status_code == 429:
                _direct_log("Serper rate limited. Waiting 30s...")
                time.sleep(30)
        except Exception as e:
            _direct_log(f"Serper error for {company_name}: {e}")
        time.sleep(0.3)

    return results[:MAX_TR1_PER_COMPANY]


def _fetch_page_text(url):
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml",
        }
        resp = httpx.get(url, timeout=15, follow_redirects=True, headers=headers)
        if resp.status_code != 200:
            return "", ""
        soup = BeautifulSoup(resp.text, "html.parser")
        title = soup.find("title")
        title_text = title.text.strip() if title else ""
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        return title_text, text[:PAGE_MAX_CHARS]
    except Exception:
        return "", ""


# ── LLM extraction ────────────────────────────────────────────────

def _extract_with_llm(page_text, url, title, company_name):
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

    prompt = f"""Analyze this TR1 (Notification of Major Holdings) announcement for {company_name}.

Page title: {title}
URL: {url}

Content:
{page_text}

Extract for each person/entity:
- name: Full name
- issuer: Company whose shares are held
- holding_pct: Percentage of voting rights
- num_shares: Number of shares
- notification_date: Date
- reason: Brief reason
- entity_type: "Individual" or "Organisation"

Extract both individuals AND organisations. Return ONLY a JSON array. If nothing found: []"""

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
                return _parse_response(msg.content[0].text if msg.content else "")
        except Exception as e:
            _direct_log(f"{prov_name} error: {e}")
    return []


def _parse_response(text):
    text = text.strip()
    m = re.search(r'\[.*\]', text, re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except json.JSONDecodeError:
            pass
    try:
        r = json.loads(text)
        return r if isinstance(r, list) else []
    except json.JSONDecodeError:
        return []


def _save_investors(investors):
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

def run_tr1_direct(max_market_cap=0):
    """Run TR1 scan filtered by market cap.
    
    Args:
        max_market_cap: Max market cap in £M. 0 = all companies.
    """
    with _direct_lock:
        if _direct_state["running"]:
            return False
        _direct_state["stop_requested"] = False
        _direct_state.update({
            "running": True,
            "max_market_cap": max_market_cap,
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "phase": "loading",
            "phase_detail": "Loading companies...",
            "companies_total": 0,
            "companies_checked": 0,
            "companies_qualified": 0,
            "companies_with_tr1": 0,
            "investors_found": 0,
            "investors_saved": 0,
            "investors_duplicate": 0,
            "skipped_large": 0,
            "skipped_no_data": 0,
            "progress_pct": 0,
            "error": None,
            "log": [],
        })

    def _run():
        try:
            serper_key = os.environ.get("SERPER_API_KEY", "")
            if not serper_key:
                _direct_log("SERPER_API_KEY not set.")
                _direct_update(phase="error", phase_detail="SERPER_API_KEY not set.",
                              error="No API key", running=False, finished_at=datetime.now().isoformat())
                return

            _init_tables()
            companies = _load_companies()
            if not companies:
                _direct_update(phase="error", phase_detail="No companies list.",
                              error="Missing lse_companies.json", running=False,
                              finished_at=datetime.now().isoformat())
                return

            # Get already-searched companies (within 7 days)
            already_searched = _get_searched_companies()
            remaining = [c for c in companies if c["name"] not in already_searched]

            _direct_log(f"Loaded {len(companies)} companies. {len(already_searched)} already searched. {len(remaining)} remaining.")
            _direct_update(companies_total=len(companies))

            if not remaining:
                _direct_log("All companies searched within last 7 days.")
                _direct_update(
                    phase="done",
                    phase_detail=f"All {len(companies)} companies searched. Will rescan after 7 days.",
                    running=False, finished_at=datetime.now().isoformat(),
                    progress_pct=100,
                )
                return

            max_mcap = _direct_state["max_market_cap"]
            today_str = date.today().isoformat()
            grand_inserted = 0
            grand_duplicated = 0
            grand_found = 0
            skipped_large = 0
            skipped_no_data = 0
            companies_qualified = 0
            companies_with_tr1 = 0

            for i, company in enumerate(remaining):
                # Check for stop
                with _direct_lock:
                    if _direct_state["stop_requested"]:
                        _direct_log("Stop requested. Saving progress.")
                        break

                name = company["name"]
                ticker = company.get("ticker", "")
                progress_pct = int(((len(already_searched) + i) / len(companies)) * 100)

                _direct_update(
                    phase="filtering",
                    phase_detail=f"Checking market cap ({i+1}/{len(remaining)}): {name[:35]}... | Qualified: {companies_qualified} | New: {grand_inserted}",
                    companies_checked=i + 1,
                    progress_pct=progress_pct,
                )

                # Market cap check
                if max_mcap > 0 and ticker:
                    mcap_m, found = _get_market_cap(ticker, name)
                    if found and mcap_m > max_mcap:
                        skipped_large += 1
                        _mark_searched(name, ticker, mcap_m, 0)
                        _direct_update(skipped_large=skipped_large)
                        continue
                    elif not found:
                        skipped_no_data += 1
                        # Still search — might be a small company with no yfinance data
                        _direct_update(skipped_no_data=skipped_no_data)
                    time.sleep(0.2)

                companies_qualified += 1
                _direct_update(
                    phase="extracting",
                    phase_detail=f"Searching TR1s ({i+1}/{len(remaining)}): {name[:35]}... | Qualified: {companies_qualified} | New: {grand_inserted}",
                    companies_qualified=companies_qualified,
                )

                # Search for TR1 filings
                tr1_pages = _search_company_tr1(name, serper_key)
                company_investors = 0

                if tr1_pages:
                    for page in tr1_pages:
                        # Check for stop
                        with _direct_lock:
                            if _direct_state["stop_requested"]:
                                break

                        title, page_text = _fetch_page_text(page["url"])
                        if not page_text or len(page_text.strip()) < 200:
                            continue

                        extracted = _extract_with_llm(page_text, page["url"], title, name)
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
                                    "context_quote": f"TR1 ({entity_type}): {item.get('reason', 'Major holding')}. {item.get('holding_pct', 'N/A')} ({item.get('num_shares', 'N/A')} shares). {item.get('notification_date', '')}",
                                    "linkedin_url": None,
                                    "date_found": today_str,
                                })

                            if investors:
                                ins, dup = _save_investors(investors)
                                grand_inserted += ins
                                grand_duplicated += dup
                                grand_found += len(investors)
                                company_investors += ins

                        time.sleep(0.5)

                if company_investors > 0:
                    companies_with_tr1 += 1
                    _direct_log(f"{name}: {company_investors} new investors")

                mcap_val = _mcap_cache.get(ticker.upper(), (0, False))[0] if ticker else 0
                _mark_searched(name, ticker, mcap_val, company_investors)

                _direct_update(
                    investors_found=grand_found,
                    investors_saved=grand_inserted,
                    investors_duplicate=grand_duplicated,
                    companies_with_tr1=companies_with_tr1,
                )

            # Done
            total_searched, recent_searched = _get_progress_stats()
            cap_msg = f" (under £{max_mcap}M)" if max_mcap > 0 else ""
            _direct_log(f"Done. {grand_inserted} new investors from {companies_with_tr1} companies{cap_msg}. Skipped: {skipped_large} large, {skipped_no_data} no data.")

            _direct_update(
                phase="done",
                phase_detail=f"Done: {grand_inserted} new from {companies_with_tr1} companies{cap_msg}. {total_searched}/{len(companies)} total searched. Skipped {skipped_large} large caps.",
                running=False,
                finished_at=datetime.now().isoformat(),
                progress_pct=int((total_searched / len(companies)) * 100) if companies else 0,
            )

        except Exception as e:
            _direct_log(f"Error: {e}")
            _direct_update(
                phase="error", phase_detail=str(e), error=str(e),
                running=False, finished_at=datetime.now().isoformat(),
            )

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return True
