"""tr1_scanner.py -- TR1 RNS announcement scanner.

Searches for TR1 (Notification of Major Holdings) announcements
from the London Stock Exchange RNS feed via Investegate and the
LSE news API. Extracts individual investor/shareholder names,
companies, and holding details.
"""

import json
import os
import re
import threading
import time
from datetime import datetime, date

import httpx
from bs4 import BeautifulSoup


# ── Scan state ────────────────────────────────────────────────────
_tr1_lock = threading.Lock()
_tr1_state = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "phase": "idle",
    "phase_detail": "",
    "announcements_found": 0,
    "pages_fetched": 0,
    "investors_found": 0,
    "investors_saved": 0,
    "investors_duplicate": 0,
    "error": None,
    "log": [],
}


def get_tr1_scan_status():
    with _tr1_lock:
        return dict(_tr1_state)


def _tr1_update(**kwargs):
    with _tr1_lock:
        _tr1_state.update(kwargs)


def _tr1_log(msg):
    with _tr1_lock:
        _tr1_state["log"].append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
        if len(_tr1_state["log"]) > 150:
            _tr1_state["log"] = _tr1_state["log"][-150:]
    print(f"[tr1_scanner] {msg}")


# ── Investegate search URLs ───────────────────────────────────────
# Investegate allows searching RNS announcements by type.
# TR1 announcements have titles containing "TR1" or "TR-1" or
# "Notification of Major Holdings"

INVESTEGATE_SEARCH_URL = "https://www.investegate.co.uk/Index.aspx"
INVESTEGATE_ANN_BASE = "https://www.investegate.co.uk"

# We search for TR1 announcements across multiple pages
# Investegate search: ?SearchType=2&SearchText=TR-1+notification
# Also search the LSE news API for additional coverage

# ── Search strategies ─────────────────────────────────────────────
# 1. Investegate advanced search for TR1 announcements
# 2. Google/Serper search for TR1 announcements on various sites
# 3. LSE API for recent RNS announcements

SERPER_TR1_QUERIES = [
    'site:investegate.co.uk "TR-1" "notification of major holdings"',
    'site:investegate.co.uk "TR1" "notification of major holdings" 2026',
    'site:investegate.co.uk "TR-1" "notification of major holdings" 2025',
    'site:investegate.co.uk "TR-1" "notification of major holdings" 2024',
    'site:investegate.co.uk "TR1" "notification" "major holdings" 2023',
    'site:investegate.co.uk "TR-1" "notification" "major holdings" 2022',
    'site:investegate.co.uk "TR1" "notification" "major holdings" 2021',
    'site:investegate.co.uk "TR1" "notification" "major holdings" 2020',
    # FT Markets also publishes TR1s
    'site:markets.ft.com "TR-1" "notification of major holdings"',
    'site:markets.ft.com "TR1" "notification of major holdings" 2025 OR 2026',
    # Direct LSE
    '"TR-1" "notification of major holdings" investor shareholder acquired',
    '"TR1" "major holdings" individual investor UK shares voting rights',
    'RNS "notification of major holdings" individual shareholder name UK',
    'RNS TR1 notification individual investor shareholder AIM listed',
    '"notification of major holdings" individual investor UK 2025 OR 2026',
    '"notification of major holdings" individual investor UK 2023 OR 2024',
    '"notification of major holdings" individual investor UK 2021 OR 2022',
    '"notification of major holdings" individual investor UK 2019 OR 2020',
]

# Max TR1 pages to fetch and analyze
TR1_PAGE_FETCH_LIMIT = 100
TR1_PAGE_MAX_CHARS = 10000  # TR1 forms can be long


def _fetch_page_text(url):
    """Fetch a web page and extract readable text."""
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
            # Remove non-content elements
            for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
                tag.decompose()
            text = soup.get_text(separator="\n", strip=True)
            return text[:TR1_PAGE_MAX_CHARS]
    except Exception:
        return ""


def _search_serper(queries):
    """Search for TR1 announcements via Serper API."""
    serper_key = os.environ.get("SERPER_API_KEY", "")
    if not serper_key:
        _tr1_log("SERPER_API_KEY not set. Skipping search.")
        return []

    results = []
    seen_urls = set()

    for i, query in enumerate(queries):
        _tr1_update(
            phase_detail=f"Searching for TR1 announcements ({i+1}/{len(queries)})..."
        )
        try:
            resp = httpx.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": serper_key, "Content-Type": "application/json"},
                json={"q": query, "num": 20},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("organic", []):
                    url = item.get("link", "")
                    if url not in seen_urls and _is_tr1_url(url):
                        seen_urls.add(url)
                        results.append({
                            "title": item.get("title", ""),
                            "url": url,
                            "snippet": item.get("snippet", ""),
                        })
            elif resp.status_code == 429:
                _tr1_log("Serper rate limited. Waiting 30s...")
                time.sleep(30)
        except Exception as e:
            _tr1_log(f"Serper error: {e}")
        time.sleep(0.3)

    _tr1_log(f"Serper found {len(results)} TR1 announcement URLs")
    return results


def _is_tr1_url(url):
    """Check if a URL is likely a TR1 announcement page."""
    url_lower = url.lower()
    if "investegate.co.uk" in url_lower and "announcement" in url_lower:
        return True
    if "markets.ft.com" in url_lower and "announce" in url_lower:
        return True
    if "londonstockexchange.com" in url_lower and "news-article" in url_lower:
        return True
    # Also allow generic pages that mention TR1
    if "tr1" in url_lower or "tr-1" in url_lower:
        return True
    if "notification" in url_lower and "major" in url_lower:
        return True
    return False


def _extract_tr1_data_with_llm(page_text, url, title):
    """Use Gemini (primary) or Anthropic (fallback) to extract investor data from TR1 text."""
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

    prompt = f"""Analyze this TR1 (Notification of Major Holdings) announcement and extract the investor/shareholder details.

A TR1 form is a regulatory filing when someone acquires or disposes of a significant shareholding in a UK-listed company.

Page title: {title}
Page URL: {url}

Page content:
{page_text}

Extract the following for each person/entity named in the notification:
- name: Full name of the person or entity with the holding (from field 3 or field 4)
- issuer: Name of the company whose shares are held (from field 1a)
- holding_pct: Percentage of voting rights held (from field 7, "Resulting situation")
- num_shares: Number of voting rights/shares held
- notification_date: Date of the notification (from field 6)
- reason: Brief reason (acquisition, disposal, event changing breakdown)

IMPORTANT:
- Only extract INDIVIDUAL PERSONS (natural persons), not corporate entities, funds, or investment firms
- If the notifier is a company/fund but field 9 names an "ultimate controlling natural person", extract that person
- Skip entries where only corporate entities are named with no individual persons
- If no individual persons are identifiable, return an empty array

Return ONLY a JSON array. Example:
[{{"name": "John Smith", "issuer": "Acme PLC", "holding_pct": "5.2%", "num_shares": "1500000", "notification_date": "2025-03-15", "reason": "Acquisition of voting rights"}}]

If no individual investors found, return: []"""

    # Try Gemini first
    if gemini_key:
        try:
            resp = httpx.post(
                "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent",
                headers={"x-goog-api-key": gemini_key, "Content-Type": "application/json"},
                json={"contents": [{"parts": [{"text": prompt}]}]},
                timeout=60,
            )
            if resp.status_code == 200:
                data = resp.json()
                text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                return _parse_llm_response(text)
            else:
                _tr1_log(f"Gemini returned {resp.status_code}")
        except Exception as e:
            _tr1_log(f"Gemini error: {e}")

    # Fallback to Anthropic
    if anthropic_key:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=anthropic_key)
            msg = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2000,
                messages=[{"role": "user", "content": prompt}],
            )
            text = msg.content[0].text if msg.content else ""
            return _parse_llm_response(text)
        except Exception as e:
            _tr1_log(f"Anthropic error: {e}")

    return []


def _parse_llm_response(text):
    """Parse JSON array from LLM response."""
    text = text.strip()
    # Find JSON array in response
    match = re.search(r'\[.*\]', text, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    # Try the whole text
    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
    except json.JSONDecodeError:
        pass
    return []


def _save_tr1_investors(investors):
    """Save TR1-sourced investors to the PostgreSQL database."""
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

def run_tr1_scan():
    """Execute a TR1 announcement scan. Runs in a background thread."""
    with _tr1_lock:
        if _tr1_state["running"]:
            return False
        _tr1_state.update({
            "running": True,
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "phase": "searching",
            "phase_detail": "Starting TR1 announcement scan...",
            "announcements_found": 0,
            "pages_fetched": 0,
            "investors_found": 0,
            "investors_saved": 0,
            "investors_duplicate": 0,
            "error": None,
            "log": [],
        })

    def _run():
        try:
            today = date.today().isoformat()

            # Step 1: Search for TR1 announcements
            _tr1_log("Searching for TR1 announcements...")
            _tr1_update(phase="searching", phase_detail="Searching for TR1 announcements via Serper...")

            results = _search_serper(SERPER_TR1_QUERIES)
            _tr1_update(announcements_found=len(results))
            _tr1_log(f"Found {len(results)} TR1 announcement URLs")

            if not results:
                _tr1_update(
                    phase="done",
                    phase_detail="No TR1 announcements found. Check SERPER_API_KEY.",
                    running=False,
                    finished_at=datetime.now().isoformat(),
                )
                return

            # Step 2: Fetch and analyze each TR1 page
            all_investors = []
            pages_fetched = 0
            consecutive_errors = 0

            # Limit to top N pages
            pages_to_fetch = results[:TR1_PAGE_FETCH_LIMIT]

            for i, result in enumerate(pages_to_fetch):
                _tr1_update(
                    phase="extracting",
                    phase_detail=f"Analyzing TR1 ({i+1}/{len(pages_to_fetch)}): {result['title'][:50]}...",
                )

                page_text = _fetch_page_text(result["url"])
                if not page_text or len(page_text.strip()) < 200:
                    _tr1_log(f"Page {i+1}: too little content from {result['url'][:60]}")
                    continue

                pages_fetched += 1
                _tr1_log(f"Page {i+1}: fetched {len(page_text)} chars")

                try:
                    extracted = _extract_tr1_data_with_llm(
                        page_text, result["url"], result["title"]
                    )
                    consecutive_errors = 0

                    if extracted:
                        _tr1_log(f"Page {i+1}: found {len(extracted)} individual(s)")
                        for item in extracted:
                            investor = {
                                "name": item.get("name", "").strip(),
                                "role": f"Shareholder ({item.get('holding_pct', 'N/A')})",
                                "company": "Independent",
                                "eis_company": item.get("issuer", "").strip(),
                                "sector": "Listed Company",
                                "amount": f"{item.get('num_shares', 'N/A')} shares",
                                "source_url": result["url"],
                                "source_type": "Filing",
                                "source_name": "LSE TR1 Filing",
                                "context_quote": f"TR1 notification: {item.get('reason', 'Major holding')}. Holding: {item.get('holding_pct', 'N/A')} ({item.get('num_shares', 'N/A')} shares). Date: {item.get('notification_date', 'N/A')}",
                                "linkedin_url": None,
                                "date_found": today,
                            }
                            # Only add if we have a name and issuer
                            if investor["name"] and investor["eis_company"]:
                                all_investors.append(investor)
                    else:
                        _tr1_log(f"Page {i+1}: no individual investors found")

                except Exception as e:
                    consecutive_errors += 1
                    _tr1_log(f"Page {i+1} extraction error: {str(e)[:120]}")
                    if consecutive_errors >= 5:
                        _tr1_log("Aborting after 5 consecutive extraction errors.")
                        break

                _tr1_update(
                    pages_fetched=pages_fetched,
                    investors_found=len(all_investors),
                )
                time.sleep(0.5)

            _tr1_log(f"Analyzed {pages_fetched} pages. Found {len(all_investors)} individual investors.")

            if not all_investors:
                _tr1_update(
                    phase="done",
                    phase_detail=f"Analyzed {pages_fetched} TR1 announcements. No new individual investors found.",
                    running=False,
                    finished_at=datetime.now().isoformat(),
                )
                return

            # Step 3: Save to database
            _tr1_update(
                phase="saving",
                phase_detail=f"Saving {len(all_investors)} investor(s) to database...",
            )
            inserted, duplicated = _save_tr1_investors(all_investors)
            _tr1_log(f"Saved: {inserted} new, {duplicated} duplicates")

            _tr1_update(
                phase="done",
                phase_detail=f"Done. {inserted} new investor(s), {duplicated} duplicate(s). Analyzed {pages_fetched} TR1 announcements.",
                running=False,
                finished_at=datetime.now().isoformat(),
                investors_saved=inserted,
                investors_duplicate=duplicated,
            )

        except Exception as e:
            _tr1_log(f"Scan error: {e}")
            _tr1_update(
                phase="error",
                phase_detail=str(e),
                error=str(e),
                running=False,
                finished_at=datetime.now().isoformat(),
            )

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return True
