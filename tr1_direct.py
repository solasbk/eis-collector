"""tr1_direct.py -- TR1 scan for LSE companies filtered by market cap.

Optimized pipeline:
1. Pre-filter LSE list to equities only (skip bonds, ETFs, warrants)
2. Check market cap via yfinance (cached in DB across restarts)
3. Search Investegate for TR1 filings per qualifying company
4. Batch Gemini extraction (2-3 pages per call)
5. Parallel company processing (3 concurrent workers)

Progress tracked per-company in the database.
"""

import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────
PARALLEL_WORKERS = 3      # companies processed in parallel
MAX_TR1_PER_COMPANY = 5   # max TR1 pages per company
PAGE_MAX_CHARS = 8000
GEMINI_BATCH_PAGES = 3    # TR1 pages per Gemini call

# Keywords that indicate a real equity (not bonds, ETFs, warrants)
EQUITY_KEYWORDS = ["PLC", "LTD", "LIMITED", "GROUP", "HOLDINGS",
                   "CORPORATION", "INC", "CORP", "CAPITAL"]
SKIP_KEYWORDS = ["NOTES", "BDS ", "BOND", "STRIP", "WARRANT", "ETF",
                 "TRACKER", "ISHARES", "SPDR", "VANGUARD", "INVESCO",
                 "WISDOMTREE", "LYXOR", "AMUNDI", "UBS ETF", "JPM ",
                 "TREASURY", "GILT", "DEBENTURE", "DB X-TRACKERS",
                 "CONCEPT FUND", "SOURCE"]

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


# ── Load and pre-filter company list ──────────────────────────────

def _load_companies():
    """Load LSE companies and filter to equities only."""
    path = Path(__file__).parent / "lse_companies.json"
    if not path.exists():
        return []
    with open(path) as f:
        all_companies = json.load(f)

    # Filter to likely equities
    equities = []
    for c in all_companies:
        name_upper = c["name"].upper()
        # Skip bonds, ETFs, warrants, etc.
        if any(kw in name_upper for kw in SKIP_KEYWORDS):
            continue
        # Keep if has equity keyword or is short name (likely equity)
        if any(kw in name_upper for kw in EQUITY_KEYWORDS) or len(c["name"].split()) <= 4:
            equities.append(c)

    return equities


# ── Database: progress tracking + market cap cache ────────────────

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
            mcap_found BOOLEAN DEFAULT FALSE,
            last_searched TIMESTAMP,
            tr1_investors_found INTEGER DEFAULT 0
        )
    """)
    # Add mcap_found column if it doesn't exist (upgrade path)
    try:
        cur.execute("ALTER TABLE tr1_company_progress ADD COLUMN IF NOT EXISTS mcap_found BOOLEAN DEFAULT FALSE")
    except Exception:
        pass
    db.commit()
    cur.close()
    db.close()


def _get_searched_companies():
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT company_name FROM tr1_company_progress WHERE last_searched > NOW() - INTERVAL '7 days'")
    result = set(row["company_name"] for row in cur.fetchall())
    cur.close()
    db.close()
    return result


def _get_recently_searched(hours=24):
    """Get companies searched within the last N hours."""
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute(f"SELECT company_name FROM tr1_company_progress WHERE last_searched > NOW() - INTERVAL '{hours} hours'")
    result = set(row["company_name"] for row in cur.fetchall())
    cur.close()
    db.close()
    return result


def _get_cached_mcaps():
    """Load market cap cache from database."""
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT ticker, market_cap_m, mcap_found FROM tr1_company_progress WHERE ticker IS NOT NULL AND ticker != ''")
    cache = {}
    for row in cur.fetchall():
        cache[row["ticker"].upper()] = (row["market_cap_m"], bool(row["mcap_found"]))
    cur.close()
    db.close()
    return cache


def _mark_searched(company_name, ticker, market_cap_m, mcap_found, tr1_count):
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO tr1_company_progress (company_name, ticker, market_cap_m, mcap_found, last_searched, tr1_investors_found)
        VALUES (%s, %s, %s, %s, NOW(), %s)
        ON CONFLICT (company_name)
        DO UPDATE SET ticker = EXCLUDED.ticker, market_cap_m = EXCLUDED.market_cap_m,
                      mcap_found = EXCLUDED.mcap_found, last_searched = NOW(),
                      tr1_investors_found = EXCLUDED.tr1_investors_found
    """, (company_name, ticker, market_cap_m, mcap_found, tr1_count))
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


# ── Market cap lookup (with DB cache) ─────────────────────────────

_mcap_cache = {}

def _get_market_cap(ticker, company_name):
    """Look up market cap in £M. Returns (mcap_m, found). Uses DB cache."""
    key = ticker.upper()
    if key in _mcap_cache:
        return _mcap_cache[key]

    try:
        import yfinance as yf
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


# ── TR1 search and extraction ─────────────────────────────────────

def _search_company_tr1(company_name, serper_key):
    """Search for TR1 filings for a company. Returns list of {title, url, snippet}."""
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
                        results.append({"title": item.get("title", ""), "url": url, "snippet": item.get("snippet", "")})
            elif resp.status_code == 429:
                _direct_log("Serper rate limited. Waiting 30s...")
                time.sleep(30)
        except Exception as e:
            _direct_log(f"Serper error for {company_name}: {e}")
        time.sleep(0.2)

    return results[:MAX_TR1_PER_COMPANY]


def _fetch_page_text(url):
    try:
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36", "Accept": "text/html,application/xhtml+xml"}
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


def _extract_batch_with_llm(pages_data, company_name):
    """Extract investors from multiple TR1 pages in a single Gemini call."""
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

    # Build combined prompt
    pages_text = ""
    for idx, (title, text, url) in enumerate(pages_data, 1):
        pages_text += f"\n--- PAGE {idx} ---\nTitle: {title}\nURL: {url}\n\n{text}\n"

    prompt = f"""Analyze these {len(pages_data)} TR1 (Notification of Major Holdings) announcements for {company_name}.

{pages_text}

For EACH page, extract all persons and entities:
- name: Full name
- issuer: Company whose shares are held
- holding_pct: Percentage of voting rights
- num_shares: Number of shares
- notification_date: Date
- reason: Brief reason
- entity_type: "Individual" or "Organisation"

Extract both individuals AND organisations across ALL pages. Return a single flat JSON array combining results from all pages. If nothing found: []"""

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
                    timeout=90,
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
                    max_tokens=3000,
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


# ── Process a single company (called in parallel) ─────────────────

def _process_company(company, serper_key, max_mcap, today_str):
    """Process one company: check mcap → search TR1 → extract → save.
    Returns (name, ticker, mcap_m, mcap_found, status, investors_inserted)
    status: 'skipped_large', 'skipped_nodata_searched', 'searched', 'error'
    """
    name = company["name"]
    ticker = company.get("ticker", "")

    try:
        # Market cap check
        mcap_m, mcap_found = 0, False
        if max_mcap > 0 and ticker:
            mcap_m, mcap_found = _get_market_cap(ticker, name)
            if mcap_found and mcap_m > max_mcap:
                return (name, ticker, mcap_m, True, "skipped_large", 0)

        # Search for TR1 filings
        tr1_pages = _search_company_tr1(name, serper_key)
        if not tr1_pages:
            return (name, ticker, mcap_m, mcap_found, "searched", 0)

        # Fetch page content
        pages_data = []
        for page in tr1_pages:
            title, text = _fetch_page_text(page["url"])
            if text and len(text.strip()) >= 200:
                pages_data.append((title, text, page["url"]))

        if not pages_data:
            return (name, ticker, mcap_m, mcap_found, "searched", 0)

        # Batch extract with Gemini (send up to GEMINI_BATCH_PAGES at once)
        all_investors = []
        for batch_start in range(0, len(pages_data), GEMINI_BATCH_PAGES):
            batch = pages_data[batch_start:batch_start + GEMINI_BATCH_PAGES]
            extracted = _extract_batch_with_llm(batch, name)
            if extracted:
                for item in extracted:
                    entity_type = item.get("entity_type", "Individual")
                    inv_name = item.get("name", "").strip()
                    issuer = item.get("issuer", "").strip() or name
                    if not inv_name:
                        continue
                    # Use the first page URL as source for the batch
                    source_url = batch[0][2] if batch else ""
                    all_investors.append({
                        "name": inv_name,
                        "role": f"Shareholder ({item.get('holding_pct', 'N/A')})",
                        "company": entity_type,
                        "eis_company": issuer,
                        "sector": "Listed Company",
                        "amount": f"{item.get('num_shares', 'N/A')} shares",
                        "source_url": source_url,
                        "source_type": "Filing",
                        "source_name": "LSE TR1 Filing",
                        "context_quote": f"TR1 ({entity_type}): {item.get('reason', 'Major holding')}. {item.get('holding_pct', 'N/A')} ({item.get('num_shares', 'N/A')} shares). {item.get('notification_date', '')}",
                        "linkedin_url": None,
                        "date_found": today_str,
                    })
            time.sleep(0.3)

        if all_investors:
            ins, dup = _save_investors(all_investors)
            return (name, ticker, mcap_m, mcap_found, "searched", ins)

        return (name, ticker, mcap_m, mcap_found, "searched", 0)

    except Exception as e:
        _direct_log(f"Error processing {name}: {e}")
        return (name, ticker, 0, False, "error", 0)


# ── Main entry point ──────────────────────────────────────────────

def run_tr1_direct(max_market_cap=0, recent_only=False):
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
            "companies_total": 0, "companies_checked": 0,
            "companies_qualified": 0, "companies_with_tr1": 0,
            "investors_found": 0, "investors_saved": 0, "investors_duplicate": 0,
            "skipped_large": 0, "skipped_no_data": 0,
            "progress_pct": 0, "error": None, "log": [],
        })

    def _run():
        try:
            serper_key = os.environ.get("SERPER_API_KEY", "")
            if not serper_key:
                _direct_update(phase="error", phase_detail="SERPER_API_KEY not set.",
                              error="No API key", running=False, finished_at=datetime.now().isoformat())
                return

            _init_tables()

            # Load and pre-filter to equities
            companies = _load_companies()
            if not companies:
                _direct_update(phase="error", phase_detail="No companies list.",
                              running=False, finished_at=datetime.now().isoformat())
                return

            # Load DB-cached market caps into memory
            global _mcap_cache
            _mcap_cache.update(_get_cached_mcaps())
            _direct_log(f"Loaded {len(_mcap_cache)} cached market caps from DB")

            # Filter out already-searched companies
            if recent_only:
                # Recent mode: only rescan companies searched > 24 hours ago
                already_searched = _get_recently_searched(hours=24)
                _direct_log(f"Recent mode: skipping {len(already_searched)} companies searched in last 24h")
            else:
                already_searched = _get_searched_companies()  # 7-day window
            remaining = [c for c in companies if c["name"] not in already_searched]

            _direct_log(f"{len(companies)} equities (filtered from full list). {len(already_searched)} already searched. {len(remaining)} remaining.")
            _direct_update(companies_total=len(companies))

            if not remaining:
                _direct_update(
                    phase="done",
                    phase_detail=f"All {len(companies)} companies searched. Rescan after 7 days.",
                    running=False, finished_at=datetime.now().isoformat(), progress_pct=100,
                )
                return

            max_mcap = _direct_state["max_market_cap"]
            today_str = date.today().isoformat()
            grand_inserted = 0
            grand_duplicated = 0
            skipped_large = 0
            companies_qualified = 0
            companies_with_tr1 = 0
            checked = 0

            # Process companies in parallel batches
            batch_idx = 0
            while batch_idx < len(remaining):
                # Check for stop
                with _direct_lock:
                    if _direct_state["stop_requested"]:
                        _direct_log("Stop requested.")
                        break

                batch = remaining[batch_idx:batch_idx + PARALLEL_WORKERS]
                batch_idx += len(batch)

                progress_pct = int(((len(already_searched) + checked) / len(companies)) * 100)
                _direct_update(
                    phase="extracting",
                    phase_detail=f"Processing ({checked+1}-{checked+len(batch)}/{len(remaining)}): {batch[0]['name'][:30]}... | Qualified: {companies_qualified} | New: {grand_inserted}",
                    companies_checked=checked,
                    progress_pct=progress_pct,
                )

                # Run batch in parallel
                with ThreadPoolExecutor(max_workers=PARALLEL_WORKERS) as executor:
                    futures = {
                        executor.submit(_process_company, c, serper_key, max_mcap, today_str): c
                        for c in batch
                    }

                    for future in as_completed(futures):
                        name, ticker, mcap_m, mcap_found, status, ins = future.result()
                        checked += 1

                        _mark_searched(name, ticker, mcap_m, mcap_found, ins)

                        if status == "skipped_large":
                            skipped_large += 1
                        else:
                            companies_qualified += 1
                            if ins > 0:
                                companies_with_tr1 += 1
                                grand_inserted += ins
                                _direct_log(f"{name}: {ins} new investors")

                _direct_update(
                    investors_saved=grand_inserted,
                    companies_qualified=companies_qualified,
                    companies_with_tr1=companies_with_tr1,
                    skipped_large=skipped_large,
                    companies_checked=checked,
                )

            # Done
            total_searched, _ = _get_progress_stats()
            cap_msg = f" (under £{max_mcap}M)" if max_mcap > 0 else ""
            _direct_log(f"Done. {grand_inserted} new from {companies_with_tr1} companies{cap_msg}. Skipped {skipped_large} large caps.")

            _direct_update(
                phase="done",
                phase_detail=f"Done: {grand_inserted} new from {companies_with_tr1} companies{cap_msg}. {total_searched}/{len(companies)} searched. Skipped {skipped_large} large.",
                running=False, finished_at=datetime.now().isoformat(),
                progress_pct=int((total_searched / len(companies)) * 100) if companies else 0,
            )

        except Exception as e:
            _direct_log(f"Error: {e}")
            _direct_update(phase="error", phase_detail=str(e), error=str(e),
                          running=False, finished_at=datetime.now().isoformat())

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return True
