"""tr1_direct.py -- Direct Investegate sequential scanner for TR1 filings.

Scans Investegate announcement IDs sequentially, checking each title
to find TR1 (Notification of Major Holdings) filings. This guarantees
complete coverage — every single TR1 filing on Investegate is found.

Progress is stored in the database so scans resume where they left off.
Default batch: 5,000 IDs per run (~1,000 TR1 pages to analyze).
"""

import json
import os
import re
import threading
import time
from datetime import datetime, date

import httpx
from bs4 import BeautifulSoup

# Market cap cache: company name (lowered) → market cap in millions GBP
_mcap_cache = {}

# ── Config ────────────────────────────────────────────────────────
BATCH_SIZE = 5000          # IDs to scan per run
MAX_TR1_EXTRACT = 200      # max TR1 pages to extract per run (Gemini cost control)
PAGE_MAX_CHARS = 10000
INVESTEGATE_BASE = "https://www.investegate.co.uk/announcement/rns/x/x"

# Starting ID: ~mid 2022 (where Investegate titles become specific)
# Below 7500000, all titles are generic "Investegate | Company Announcement"
# which makes title-based TR1 detection impossible.
DEFAULT_START_ID = 7500000
# Approximate current max (April 2026)
APPROX_MAX_ID = 8600000

# Title keywords that indicate a TR1 / major holdings filing
TR1_KEYWORDS = [
    "notification of major holdings",
    "holding(s) in company",
    "holdings in company",
    "tr-1",
    "tr1 ",
    "tr1:",
    "major holdings",
]

# Exclude these — they look similar but aren't TR1 filings
EXCLUDE_KEYWORDS = [
    "director/pdmr",
    "pdmr dealing",
    "director dealing",
]

# ── Scan state ────────────────────────────────────────────────────
_direct_lock = threading.Lock()
_direct_state = {
    "running": False,
    "stop_requested": False,
    "started_at": None,
    "finished_at": None,
    "phase": "idle",
    "phase_detail": "",
    "ids_scanned": 0,
    "tr1_found": 0,
    "tr1_extracted": 0,
    "investors_found": 0,
    "investors_saved": 0,
    "investors_duplicate": 0,
    "current_id": 0,
    "progress_pct": 0,
    "max_market_cap": 0,  # 0 = no filter
    "error": None,
    "log": [],
}


def stop_direct_scan():
    """Request the running scan to stop after the current batch."""
    with _direct_lock:
        if _direct_state["running"]:
            _direct_state["stop_requested"] = True
            return True
    return False


def get_direct_status():
    with _direct_lock:
        return dict(_direct_state)


def _direct_update(**kwargs):
    with _direct_lock:
        _direct_state.update(kwargs)


def _direct_log(msg):
    with _direct_lock:
        _direct_state["log"].append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
        if len(_direct_state["log"]) > 200:
            _direct_state["log"] = _direct_state["log"][-200:]
    print(f"[tr1_direct] {msg}")


# ── Database: progress tracking ───────────────────────────────────

def _init_direct_table():
    """Create the progress tracking table."""
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tr1_direct_progress (
            id SERIAL PRIMARY KEY,
            key TEXT NOT NULL UNIQUE,
            value TEXT NOT NULL
        )
    """)
    db.commit()
    cur.close()
    db.close()


def _get_last_scanned_id():
    """Get the last scanned Investegate ID."""
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("SELECT value FROM tr1_direct_progress WHERE key = 'last_scanned_id'")
    row = cur.fetchone()
    cur.close()
    db.close()
    if row:
        stored_id = int(row["value"])
        # If stored ID is below the useful range, jump to default
        if stored_id < DEFAULT_START_ID:
            return DEFAULT_START_ID
        return stored_id
    return DEFAULT_START_ID


def _set_last_scanned_id(id_num):
    """Save the last scanned ID."""
    from api_server import get_db
    db = get_db()
    cur = db.cursor()
    cur.execute("""
        INSERT INTO tr1_direct_progress (key, value)
        VALUES ('last_scanned_id', %s)
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    """, (str(id_num),))
    db.commit()
    cur.close()
    db.close()


# ── ID scanning ───────────────────────────────────────────────────

def _check_title(id_num, client):
    """Fetch just enough of a page to extract the title. Returns (is_tr1, title)."""
    try:
        url = f"{INVESTEGATE_BASE}/{id_num}"
        with client.stream("GET", url, follow_redirects=True) as resp:
            if resp.status_code != 200:
                return False, ""
            chunk = b""
            for c in resp.iter_bytes(3072):
                chunk += c
                if b"</title>" in chunk:
                    break
            text = chunk.decode("utf-8", errors="ignore")
            start = text.find("<title>")
            end = text.find("</title>")
            if start >= 0 and end >= 0:
                title = text[start + 7:end].strip().lower()
                # Check for exclusions first
                if any(kw in title for kw in EXCLUDE_KEYWORDS):
                    return False, title
                # Check for TR1 keywords
                if any(kw in title for kw in TR1_KEYWORDS):
                    return True, title
            return False, ""
    except Exception:
        return False, ""


def _extract_issuer_name(id_num):
    """Quick fetch to extract just the issuer/company name from a TR1 page.
    Reads enough of the page to find the company name without a full parse."""
    try:
        url = f"{INVESTEGATE_BASE}/{id_num}"
        resp = httpx.get(url, timeout=10, follow_redirects=True)
        if resp.status_code != 200:
            return ""
        # Look for common patterns in TR1 filings that name the issuer
        text = resp.text[:8000]
        import re
        # Pattern 1: "1. Issuer Name: COMPANY" or "1a. Name: COMPANY"
        m = re.search(r'(?:1\.?\s*(?:a\.?)?\s*(?:Issuer)?\s*Name[:\s]+)([A-Z][A-Za-z0-9\s&.,()\'-]+?)(?:\n|<|\r)', text)
        if m:
            return m.group(1).strip()
        # Pattern 2: Company name in breadcrumb or header
        soup = BeautifulSoup(text, "html.parser")
        # Check for company name in h1/h2 or specific div
        for tag in soup.find_all(["h1", "h2", "h3"]):
            t = tag.get_text().strip()
            if t and len(t) > 3 and t.lower() not in ["holding(s) in company", "notification of major holdings", "tr-1"]:
                return t
        return ""
    except Exception:
        return ""


def _lookup_market_cap(company_name):
    """Look up market cap in millions GBP. Returns 0 if unknown (treated as no filter)."""
    if not company_name:
        return 0
    
    key = company_name.lower().strip()
    if key in _mcap_cache:
        return _mcap_cache[key]
    
    try:
        import yfinance as yf
        # Try common ticker formats for UK stocks
        # Clean the name and try as ticker
        clean = company_name.upper().replace(" PLC", "").replace(" LTD", "").replace(" GROUP", "").replace(" HOLDINGS", "").strip()
        # Try direct search
        search = yf.Search(company_name, max_results=3)
        if search.quotes:
            for quote in search.quotes:
                symbol = quote.get("symbol", "")
                if symbol.endswith(".L"):  # London Stock Exchange
                    ticker = yf.Ticker(symbol)
                    info = ticker.info
                    mcap = info.get("marketCap", 0)
                    if mcap:
                        mcap_m = int(mcap / 1e6)  # Convert to millions
                        _mcap_cache[key] = mcap_m
                        return mcap_m
        _mcap_cache[key] = 0  # Cache misses too
        return 0
    except Exception:
        _mcap_cache[key] = 0
        return 0


def _fetch_full_page(id_num):
    """Fetch the full page content for extraction."""
    try:
        url = f"{INVESTEGATE_BASE}/{id_num}"
        resp = httpx.get(url, timeout=15, follow_redirects=True)
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

def _extract_with_llm(page_text, url, title):
    """Extract investor data from TR1 text using Gemini/Anthropic."""
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

    prompt = f"""Analyze this TR1 (Notification of Major Holdings) announcement and extract ALL persons and entities.

Page title: {title}
Page URL: {url}

Page content:
{page_text}

Extract for each person/entity:
- name: Full name
- issuer: Company whose shares are held
- holding_pct: Percentage of voting rights
- num_shares: Number of shares
- notification_date: Date of notification
- reason: Brief reason (acquisition, disposal, etc.)
- entity_type: "Individual" or "Organisation"

Extract both individuals AND organisations. If field 9 names an ultimate controlling person, extract them too.

Return ONLY a JSON array. If nothing found, return: []"""

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
                    _direct_log(f"Gemini {resp.status_code}")
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
            _direct_log(f"{prov_name} error: {e}")
    return []


def _parse_response(text):
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

def run_tr1_direct(max_market_cap=0):
    """Run the direct Investegate sequential scan.
    
    Args:
        max_market_cap: Max market cap in millions GBP. 0 = no filter.
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
            "phase": "scanning",
            "phase_detail": "Starting Investegate direct scan...",
            "ids_scanned": 0,
            "tr1_found": 0,
            "tr1_extracted": 0,
            "investors_found": 0,
            "investors_saved": 0,
            "investors_duplicate": 0,
            "current_id": 0,
            "progress_pct": 0,
            "error": None,
            "log": [],
        })

    def _run():
        try:
            _init_direct_table()
            total_range = APPROX_MAX_ID - DEFAULT_START_ID
            today_str = date.today().isoformat()
            grand_inserted = 0
            grand_duplicated = 0
            grand_found = 0
            grand_ids = 0
            grand_tr1 = 0
            batches_done = 0

            max_mcap = _direct_state.get("max_market_cap", 0)
            if max_mcap > 0:
                _direct_log(f"Market cap filter: only companies under £{max_mcap:,}M")

            while True:
                # Check for stop request
                with _direct_lock:
                    if _direct_state["stop_requested"]:
                        _direct_log(f"Stop requested. Saving progress at current position.")
                        break

                start_id = _get_last_scanned_id()
                end_id = min(start_id + BATCH_SIZE, APPROX_MAX_ID + 100000)

                # If we've reached the end, we're done
                if start_id >= APPROX_MAX_ID:
                    _direct_log(f"Full range complete. {grand_inserted} total new investors across {batches_done} batches.")
                    break

                progress_pct = int(((start_id - DEFAULT_START_ID) / total_range) * 100) if total_range > 0 else 0
                batches_done += 1

                _direct_log(f"Batch {batches_done}: IDs {start_id}-{end_id} ({progress_pct}%)")
                _direct_update(current_id=start_id, progress_pct=progress_pct)

                # Phase 1: Fast title scan
                tr1_ids = []
                ids_checked = 0
                _direct_update(phase="scanning", phase_detail=f"Batch {batches_done}: scanning titles {start_id}-{end_id} ({progress_pct}%)...")

                with httpx.Client(timeout=5) as client:
                    for id_num in range(start_id, end_id):
                        is_tr1, title = _check_title(id_num, client)
                        ids_checked += 1

                        if is_tr1:
                            tr1_ids.append(id_num)

                        if ids_checked % 500 == 0:
                            _direct_update(
                                phase_detail=f"Batch {batches_done} ({progress_pct}%): {ids_checked}/{BATCH_SIZE} scanned, {len(tr1_ids)} TR1 found | Total new: {grand_inserted}",
                                ids_scanned=grand_ids + ids_checked,
                                tr1_found=grand_tr1 + len(tr1_ids),
                                current_id=id_num,
                            )

                        if ids_checked % 50 == 0:
                            time.sleep(0.1)

                        # Check for stop mid-scan
                        if ids_checked % 500 == 0:
                            with _direct_lock:
                                if _direct_state["stop_requested"]:
                                    _direct_log("Stop requested during title scan.")
                                    break

                grand_ids += ids_checked
                grand_tr1 += len(tr1_ids)
                _direct_log(f"Batch {batches_done}: {ids_checked} IDs, {len(tr1_ids)} TR1 found")

                if not tr1_ids:
                    _set_last_scanned_id(end_id)
                    continue  # move to next batch

                # Phase 2: Market cap filter + Extract investor data
                pages_to_extract = tr1_ids[:MAX_TR1_EXTRACT]
                skipped_mcap = 0

                consecutive_errors = 0
                for i, id_num in enumerate(pages_to_extract):
                    # Check for stop
                    with _direct_lock:
                        if _direct_state["stop_requested"]:
                            _direct_log("Stop requested during extraction.")
                            break

                    url = f"{INVESTEGATE_BASE}/{id_num}"

                    # Market cap pre-filter: extract company name, check cap
                    if max_mcap > 0:
                        _direct_update(
                            phase="extracting",
                            phase_detail=f"Batch {batches_done} ({progress_pct}%): checking market cap {i+1}/{len(pages_to_extract)} | New: {grand_inserted} | Skipped: {skipped_mcap}",
                        )
                        issuer = _extract_issuer_name(id_num)
                        if issuer:
                            mcap = _lookup_market_cap(issuer)
                            if mcap > 0 and mcap > max_mcap:
                                skipped_mcap += 1
                                continue  # Skip — company too large

                    _direct_update(
                        phase="extracting",
                        phase_detail=f"Batch {batches_done} ({progress_pct}%): extracting TR1 {i+1}/{len(pages_to_extract)} | New: {grand_inserted} | Skipped: {skipped_mcap}",
                        tr1_extracted=grand_tr1,
                    )

                    title, page_text = _fetch_full_page(id_num)
                    if not page_text or len(page_text.strip()) < 200:
                        continue

                    try:
                        extracted = _extract_with_llm(page_text, url, title)
                        consecutive_errors = 0

                        if extracted:
                            investors = []
                            for item in extracted:
                                entity_type = item.get("entity_type", "Individual")
                                name = item.get("name", "").strip()
                                issuer = item.get("issuer", "").strip()
                                if not name or not issuer:
                                    continue
                                investors.append({
                                    "name": name,
                                    "role": f"Shareholder ({item.get('holding_pct', 'N/A')})",
                                    "company": entity_type,
                                    "eis_company": issuer,
                                    "sector": "Listed Company",
                                    "amount": f"{item.get('num_shares', 'N/A')} shares",
                                    "source_url": url,
                                    "source_type": "Filing",
                                    "source_name": "LSE TR1 Filing",
                                    "context_quote": f"TR1 direct ({entity_type}): {item.get('reason', 'Major holding')}. {item.get('holding_pct', 'N/A')} ({item.get('num_shares', 'N/A')} shares). {item.get('notification_date', '')}",
                                    "linkedin_url": None,
                                    "date_found": today_str,
                                })

                            if investors:
                                ins, dup = _save_investors(investors)
                                grand_inserted += ins
                                grand_duplicated += dup
                                grand_found += len(investors)

                    except Exception as e:
                        consecutive_errors += 1
                        _direct_log(f"Extract error ID {id_num}: {str(e)[:100]}")
                        if consecutive_errors >= 5:
                            _direct_log("5 consecutive errors. Skipping rest of this batch.")
                            break

                    _direct_update(
                        investors_found=grand_found,
                        investors_saved=grand_inserted,
                        investors_duplicate=grand_duplicated,
                    )
                    time.sleep(0.5)

                # Save progress and continue to next batch
                _set_last_scanned_id(end_id)
                progress_pct = int(((end_id - DEFAULT_START_ID) / total_range) * 100) if total_range > 0 else 0
                _direct_log(f"Batch {batches_done} done. Running total: {grand_inserted} new, {grand_duplicated} dupes. {progress_pct}% complete.")

            # All batches finished
            final_pct = int(((_get_last_scanned_id() - DEFAULT_START_ID) / total_range) * 100) if total_range > 0 else 100
            _direct_update(
                phase="done",
                phase_detail=f"Complete: {grand_inserted} new investors from {grand_tr1} TR1 filings across {grand_ids} IDs ({batches_done} batches, {final_pct}%).",
                running=False,
                finished_at=datetime.now().isoformat(),
                ids_scanned=grand_ids,
                progress_pct=final_pct,
            )

        except Exception as e:
            _direct_log(f"Scan error: {e}")
            _direct_update(
                phase="error", phase_detail=str(e), error=str(e),
                running=False, finished_at=datetime.now().isoformat(),
            )

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return True
