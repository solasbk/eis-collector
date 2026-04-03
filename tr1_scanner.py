"""tr1_scanner.py -- TR1 RNS announcement scanner.

Searches for TR1 (Notification of Major Holdings) announcements
from the London Stock Exchange RNS feed. Uses Serper to find
TR1 pages on Investegate, FT Markets, and other financial sites,
then extracts investor/shareholder data using Gemini.

Supports configurable date ranges via the days_back parameter.
"""

import json
import os
import re
import threading
import time
from datetime import datetime, date, timedelta

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
    "days_back": 30,
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
        if len(_tr1_state["log"]) > 200:
            _tr1_state["log"] = _tr1_state["log"][-200:]
    print(f"[tr1_scanner] {msg}")


# ── Page limits ───────────────────────────────────────────────────
TR1_PAGE_FETCH_LIMIT = 150
TR1_PAGE_MAX_CHARS = 10000


def _build_queries(days_back=30):
    """Build a comprehensive set of Serper queries for TR1 announcements.
    
    Generates queries across:
    - Multiple sites (Investegate, FT Markets, company websites)
    - Different date ranges based on days_back
    - Various TR1 title formats
    - Specific sectors and company types
    """
    queries = []
    
    # Core TR1 search terms
    tr1_terms = [
        '"TR-1" "notification of major holdings"',
        '"TR1" "notification of major holdings"',
        '"TR-1" "notification" "major holdings"',
        '"notification of major holdings" "voting rights"',
        '"TR-1" notification shareholder holdings',
        '"Holding(s) in Company" RNS',
        '"Holdings in Company" notification',
    ]
    
    # Sites to search
    sites = [
        "site:investegate.co.uk",
        "site:markets.ft.com",
        "site:londonstockexchange.com",
        "",  # open web
    ]
    
    # Generate year/date range queries based on days_back
    today = date.today()
    start_date = today - timedelta(days=days_back)
    
    # Get the years covered
    years = set()
    d = start_date
    while d <= today:
        years.add(d.year)
        d += timedelta(days=365)
    years.add(today.year)
    years = sorted(years)
    
    # Build queries: each site × each TR1 term × each year
    for site in sites:
        for term in tr1_terms[:4]:  # top 4 terms per site
            if years:
                year_str = " OR ".join(str(y) for y in years[-2:])  # last 2 years
                q = f"{site} {term} {year_str}".strip()
                queries.append(q)
            else:
                queries.append(f"{site} {term}".strip())
    
    # Add month-specific queries for recent months (higher precision)
    months = []
    d = start_date
    while d <= today:
        months.append(d.strftime("%B %Y"))
        d = d.replace(day=1) + timedelta(days=32)
        d = d.replace(day=1)
    
    for month in months[-6:]:  # last 6 months
        queries.append(f'site:investegate.co.uk "TR-1" "notification" "{month}"')
        queries.append(f'site:investegate.co.uk "major holdings" "{month}"')
    
    # Individual investor focused queries
    investor_queries = [
        '"TR-1" individual shareholder notification UK listed',
        '"notification of major holdings" individual investor AIM',
        '"notification of major holdings" individual shareholder "Main Market"',
        'RNS "notification of major holdings" person acquired shares UK',
        '"TR-1" notification individual "acquired" OR "disposed" voting rights',
        'investegate "TR-1" OR "TR1" individual shareholder notification',
        '"Holding(s) in Company" individual investor RNS',
        '"notification of major holdings" director shareholder UK',
        '"TR-1" significant shareholder individual person UK listed company',
    ]
    queries.extend(investor_queries)
    
    # Sector-specific TR1 queries (these often name individual investors)
    sector_queries = [
        '"TR-1" "notification" AIM technology company shareholder',
        '"TR-1" "notification" AIM mining resources shareholder',
        '"TR-1" "notification" AIM biotech pharma shareholder',
        '"TR-1" "notification" AIM oil gas energy shareholder',
        '"TR-1" "notification" AIM property real estate shareholder',
        '"TR-1" "notification" AIM fintech financial shareholder',
        '"notification of major holdings" FTSE 250 individual investor',
        '"notification of major holdings" FTSE AIM 100 shareholder',
    ]
    queries.extend(sector_queries)
    
    # Historical deep-dive queries if looking back far
    if days_back > 90:
        for year in range(max(2019, start_date.year), today.year):
            queries.append(f'site:investegate.co.uk "TR-1" "notification of major holdings" {year}')
            queries.append(f'"notification of major holdings" individual investor UK {year}')
    
    _tr1_log(f"Generated {len(queries)} search queries for {days_back} days back")
    return queries


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
        _tr1_log("SERPER_API_KEY not set.")
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

    _tr1_log(f"Found {len(results)} unique TR1 announcement URLs")
    return results


def _is_tr1_url(url):
    """Check if a URL is likely a TR1 announcement page."""
    url_lower = url.lower()
    # Known good sources
    if "investegate.co.uk" in url_lower and ("announcement" in url_lower or "rns" in url_lower):
        return True
    if "markets.ft.com" in url_lower and "announce" in url_lower:
        return True
    if "londonstockexchange.com" in url_lower and "news-article" in url_lower:
        return True
    # Generic pages mentioning TR1 in the URL
    if "tr1" in url_lower or "tr-1" in url_lower:
        return True
    if "notification" in url_lower and ("major" in url_lower or "holding" in url_lower):
        return True
    # RNS announcement pages on company websites
    if "rns" in url_lower and ("notification" in url_lower or "holding" in url_lower):
        return True
    return False


def _extract_tr1_data_with_llm(page_text, url, title):
    """Use Gemini (primary) or Anthropic (fallback) to extract investor data from TR1 text."""
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

    prompt = f"""Analyze this TR1 (Notification of Major Holdings) announcement and extract ALL persons and entities named.

A TR1 form is a regulatory filing when someone acquires or disposes of a significant shareholding in a UK-listed company.

Page title: {title}
Page URL: {url}

Page content:
{page_text}

Extract the following for each person/entity named in the notification:
- name: Full name of the person or entity (from field 3 or field 4)
- issuer: Name of the company whose shares are held (from field 1a)
- holding_pct: Percentage of voting rights held (from field 7, "Resulting situation")
- num_shares: Number of voting rights/shares held
- notification_date: Date of the notification (from field 6)
- reason: Brief reason (acquisition, disposal, event changing breakdown)
- entity_type: "Individual" for natural persons, "Organisation" for companies/funds/firms

IMPORTANT:
- Extract ALL persons and entities — both individuals AND organisations
- If field 9 names an ultimate controlling natural person, extract them as a separate Individual entry
- If no persons or entities are identifiable, return an empty array

Return ONLY a JSON array. Example:
[{{"name": "John Smith", "issuer": "Acme PLC", "holding_pct": "5.2%", "num_shares": "1500000", "notification_date": "2025-03-15", "reason": "Acquisition of voting rights", "entity_type": "Individual"}}, {{"name": "BlackRock Inc", "issuer": "Acme PLC", "holding_pct": "8.1%", "num_shares": "3200000", "notification_date": "2025-03-15", "reason": "Acquisition of voting rights", "entity_type": "Organisation"}}]

If no persons or entities found, return: []"""

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
                    return _parse_llm_response(text)
                else:
                    _tr1_log(f"Gemini returned {resp.status_code}")
            else:
                import anthropic
                client = anthropic.Anthropic(api_key=prov_key)
                msg = client.messages.create(
                    model="claude-haiku-4-5-20251001",
                    max_tokens=2000,
                    messages=[{"role": "user", "content": prompt}],
                )
                text = msg.content[0].text if msg.content else ""
                return _parse_llm_response(text)
        except Exception as e:
            _tr1_log(f"{prov_name} error: {e}")

    return []


def _parse_llm_response(text):
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

def run_tr1_scan(days_back=30):
    """Execute a TR1 announcement scan. Runs in a background thread.
    
    Args:
        days_back: How many days of history to search (7, 30, 90, 365, etc.)
    """
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
            "days_back": days_back,
        })

    def _run():
        try:
            today_str = date.today().isoformat()

            # Step 1: Build and execute search queries
            _tr1_log(f"Scanning TR1 announcements from last {days_back} days...")
            queries = _build_queries(days_back)
            
            _tr1_update(phase="searching", phase_detail=f"Searching {len(queries)} queries for TR1 announcements...")
            results = _search_serper(queries)
            _tr1_update(announcements_found=len(results))

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

            pages_to_fetch = results[:TR1_PAGE_FETCH_LIMIT]
            _tr1_log(f"Will analyze up to {len(pages_to_fetch)} TR1 pages")

            for i, result in enumerate(pages_to_fetch):
                _tr1_update(
                    phase="extracting",
                    phase_detail=f"Analyzing TR1 ({i+1}/{len(pages_to_fetch)}): {result['title'][:50]}...",
                )

                page_text = _fetch_page_text(result["url"])
                if not page_text or len(page_text.strip()) < 200:
                    continue

                pages_fetched += 1

                try:
                    extracted = _extract_tr1_data_with_llm(
                        page_text, result["url"], result["title"]
                    )
                    consecutive_errors = 0

                    if extracted:
                        _tr1_log(f"Page {i+1}: found {len(extracted)} entity/entities")
                        for item in extracted:
                            entity_type = item.get("entity_type", "Individual")
                            name = item.get("name", "").strip()
                            issuer = item.get("issuer", "").strip()
                            if not name or not issuer:
                                continue

                            investor = {
                                "name": name,
                                "role": f"Shareholder ({item.get('holding_pct', 'N/A')})",
                                "company": entity_type,
                                "eis_company": issuer,
                                "sector": "Listed Company",
                                "amount": f"{item.get('num_shares', 'N/A')} shares",
                                "source_url": result["url"],
                                "source_type": "Filing",
                                "source_name": "LSE TR1 Filing",
                                "context_quote": f"TR1 notification ({entity_type}): {item.get('reason', 'Major holding')}. Holding: {item.get('holding_pct', 'N/A')} ({item.get('num_shares', 'N/A')} shares). Date: {item.get('notification_date', 'N/A')}",
                                "linkedin_url": None,
                                "date_found": today_str,
                            }
                            all_investors.append(investor)

                except Exception as e:
                    consecutive_errors += 1
                    _tr1_log(f"Page {i+1} error: {str(e)[:120]}")
                    if consecutive_errors >= 5:
                        _tr1_log("Aborting after 5 consecutive errors.")
                        break

                _tr1_update(pages_fetched=pages_fetched, investors_found=len(all_investors))
                time.sleep(0.5)

            _tr1_log(f"Analyzed {pages_fetched} pages. Found {len(all_investors)} entities.")

            if not all_investors:
                _tr1_update(
                    phase="done",
                    phase_detail=f"Analyzed {pages_fetched} TR1 announcements. No new entities found.",
                    running=False,
                    finished_at=datetime.now().isoformat(),
                )
                return

            # Step 3: Save
            _tr1_update(phase="saving", phase_detail=f"Saving {len(all_investors)} entities...")
            inserted, duplicated = _save_tr1_investors(all_investors)
            _tr1_log(f"Saved: {inserted} new, {duplicated} duplicates")

            _tr1_update(
                phase="done",
                phase_detail=f"Done. {inserted} new, {duplicated} duplicates. Analyzed {pages_fetched} TR1 pages ({days_back} day range).",
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
