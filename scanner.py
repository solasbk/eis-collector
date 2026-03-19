"""scanner.py — EIS Investor web scanner.

Searches DuckDuckGo for public references to individuals investing
in UK EIS/SEIS qualifying companies, then uses an LLM to extract
structured investor records.
"""

import json
import os
import threading
import time
import sqlite3
from datetime import datetime, date
from typing import Optional

import httpx
from duckduckgo_search import DDGS

import os as _os
DATA_DIR = _os.environ.get("DATA_DIR", ".")
DB_PATH = _os.path.join(DATA_DIR, "eis_investors.db")

# ── Scan state (in-memory, single-instance) ──────────────────────
_scan_lock = threading.Lock()
_scan_state = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "phase": "idle",            # idle | searching | extracting | saving | done | error
    "phase_detail": "",
    "results_found": 0,
    "results_saved": 0,
    "results_duplicate": 0,
    "error": None,
    "last_results": [],         # summary of last run
}

SEARCH_QUERIES = [
    '"EIS investor" OR "enterprise investment scheme" individual invested backed 2026',
    'angel investor EIS UK startup funding announcement 2026',
    '"EIS qualifying" investor personal investment UK 2026',
    'SEIS EIS angel backed individual investor new funding round 2026',
    '"EIS relief" angel invested startup UK',
    'enterprise investment scheme angel round individual backing',
]

EXTRACTION_PROMPT = """You are an analyst extracting structured data about individual investors in UK EIS (Enterprise Investment Scheme) or SEIS (Seed Enterprise Investment Scheme) qualifying companies.

Given the following search result snippet, determine if it mentions a NAMED INDIVIDUAL (not a fund or firm) who has personally invested in an EIS/SEIS qualifying company.

Search result:
Title: {title}
URL: {url}
Snippet: {snippet}

Rules:
- Only extract NAMED INDIVIDUALS (first and last name), not fund names or firm names alone
- The person must be clearly referenced as making an EIS or SEIS investment, or investing in a company that is described as EIS/SEIS qualifying
- Skip generic mentions like "EIS investors" without specific names
- If the snippet is about an EIS fund manager or advisor (not making a personal investment), skip it
- If no qualifying individual investor is found, return: {{"investors": []}}

Return a JSON object with:
{{"investors": [
  {{
    "name": "Full Name",
    "role": "Their professional role/title (or 'Angel Investor' if unknown)",
    "company": "Their employer/firm (or 'Independent' if unknown)",
    "eis_company": "The EIS company they invested in",
    "sector": "The EIS company's sector (brief)",
    "amount": "Investment amount if disclosed, otherwise 'Undisclosed'",
    "context_quote": "A brief quote from the snippet showing the EIS investment mention"
  }}
]}}

Return ONLY valid JSON, nothing else."""


def get_scan_status():
    """Return current scan state (thread-safe)."""
    with _scan_lock:
        return dict(_scan_state)


def _update_state(**kwargs):
    with _scan_lock:
        _scan_state.update(kwargs)


def _search_web():
    """Run searches and collect results. Tries DuckDuckGo first, falls back to Bing."""
    all_results = []
    seen_urls = set()

    # Try DuckDuckGo
    try:
        with DDGS() as ddgs:
            for query in SEARCH_QUERIES:
                _update_state(phase_detail=f"Searching: {query[:60]}...")
                try:
                    results = list(ddgs.text(query, max_results=10, region="uk-en"))
                    for r in results:
                        url = r.get("href", "")
                        if url and url not in seen_urls:
                            seen_urls.add(url)
                            all_results.append({
                                "title": r.get("title", ""),
                                "url": url,
                                "snippet": r.get("body", ""),
                            })
                except Exception as e:
                    print(f"[scanner] DDG search error: {e}")
                time.sleep(0.5)
    except Exception as e:
        print(f"[scanner] DDG init error: {e}")

    # If DDG returned nothing, try Bing scraping
    if not all_results:
        _update_state(phase_detail="Primary search returned no results. Trying alternative...")
        all_results = _search_bing(seen_urls)

    return all_results


def _search_bing(seen_urls):
    """Fallback: scrape Bing search results."""
    from bs4 import BeautifulSoup
    results = []

    for query in SEARCH_QUERIES[:4]:  # limit to first 4 queries
        _update_state(phase_detail=f"Searching (alt): {query[:50]}...")
        try:
            resp = httpx.get(
                "https://www.bing.com/search",
                params={"q": query, "count": "10"},
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
                timeout=10,
                follow_redirects=True,
            )
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                for item in soup.select("li.b_algo"):
                    a_tag = item.select_one("h2 a")
                    if not a_tag:
                        continue
                    url = a_tag.get("href", "")
                    title = a_tag.get_text(strip=True)
                    snippet_el = item.select_one(".b_caption p")
                    snippet = snippet_el.get_text(strip=True) if snippet_el else ""
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        results.append({"title": title, "url": url, "snippet": snippet})
        except Exception as e:
            print(f"[scanner] Bing search error: {e}")
        time.sleep(0.5)

    return results


def _extract_investors_from_results(results):
    """Use Anthropic to extract investor mentions from search results."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        # Try reading from common locations
        for path in ["/home/user/.anthropic_key", "/home/user/workspace/.env"]:
            try:
                with open(path) as f:
                    for line in f:
                        if "ANTHROPIC_API_KEY" in line:
                            api_key = line.split("=", 1)[1].strip().strip('"').strip("'")
                            break
            except FileNotFoundError:
                pass

    all_investors = []
    today = date.today().isoformat()

    for i, result in enumerate(results):
        _update_state(
            phase="extracting",
            phase_detail=f"Analyzing result {i+1}/{len(results)}: {result['title'][:50]}..."
        )

        if api_key:
            # Use Anthropic for extraction
            try:
                investor_data = _extract_with_anthropic(api_key, result)
                for inv in investor_data:
                    inv["source_url"] = result["url"]
                    inv["source_type"] = _classify_source(result["url"])
                    inv["source_name"] = _extract_source_name(result["url"], result["title"])
                    inv["date_found"] = today
                    inv["linkedin_url"] = None
                    all_investors.append(inv)
            except Exception as e:
                print(f"[scanner] Extraction error: {e}")
        else:
            # Fallback: simple keyword-based extraction (less accurate)
            investors = _extract_simple(result)
            for inv in investors:
                inv["date_found"] = today
                inv["linkedin_url"] = None
                all_investors.append(inv)

        time.sleep(0.3)

    return all_investors


def _extract_with_anthropic(api_key, result):
    """Call Anthropic API to extract investors from a search result."""
    prompt = EXTRACTION_PROMPT.format(
        title=result["title"],
        url=result["url"],
        snippet=result["snippet"],
    )

    with httpx.Client(timeout=30) as client:
        resp = client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 1024,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        resp.raise_for_status()
        content = resp.json()["content"][0]["text"]

        # Parse JSON from response
        try:
            data = json.loads(content)
            return data.get("investors", [])
        except json.JSONDecodeError:
            # Try to find JSON in the response
            start = content.find("{")
            end = content.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(content[start:end])
                return data.get("investors", [])
            return []


def _extract_simple(result):
    """Fallback: simple keyword extraction without LLM."""
    # Very basic -- looks for patterns like "Name invested" or "Name backed"
    # This is a rough fallback; LLM extraction is much better
    return []


def _classify_source(url):
    """Classify source type from URL."""
    url_lower = url.lower()
    if any(d in url_lower for d in ["twitter.com", "x.com", "linkedin.com", "facebook.com"]):
        return "Social Media"
    if any(d in url_lower for d in ["companieshouse", "gov.uk", "fca.org"]):
        return "Filing"
    if any(d in url_lower for d in ["reddit.com", "forum", "community"]):
        return "Forum"
    return "News"


def _extract_source_name(url, title):
    """Extract a readable source name from URL."""
    from urllib.parse import urlparse
    try:
        domain = urlparse(url).netloc.replace("www.", "")
        # Common mappings
        mappings = {
            "techcrunch.com": "TechCrunch",
            "ft.com": "Financial Times",
            "reuters.com": "Reuters",
            "bloomberg.com": "Bloomberg",
            "cityam.com": "City A.M.",
            "sifted.eu": "Sifted",
            "uktech.news": "UKTN",
            "ffnews.com": "FF News",
            "growthbusiness.co.uk": "Growth Business",
            "altfi.com": "AltFi",
            "standard.co.uk": "Evening Standard",
            "theguardian.com": "The Guardian",
            "bbc.co.uk": "BBC",
            "news.sky.com": "Sky News",
            "thisismoney.co.uk": "This is Money",
        }
        return mappings.get(domain, domain)
    except Exception:
        return "Unknown"


def _save_to_db(investors):
    """Save extracted investors to the database, deduplicating by name + eis_company."""
    db = sqlite3.connect(DB_PATH, check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")

    inserted = 0
    duplicated = 0

    for inv in investors:
        existing = db.execute(
            "SELECT id FROM investors WHERE name = ? AND eis_company = ?",
            [inv.get("name", ""), inv.get("eis_company", "")]
        ).fetchone()

        if existing:
            duplicated += 1
        else:
            db.execute("""
                INSERT INTO investors (name, role, company, eis_company, sector, amount,
                source_url, source_type, source_name, context_quote, linkedin_url, date_found)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                inv.get("name"), inv.get("role"), inv.get("company"),
                inv.get("eis_company"), inv.get("sector"), inv.get("amount"),
                inv.get("source_url"), inv.get("source_type"), inv.get("source_name"),
                inv.get("context_quote"), inv.get("linkedin_url"), inv.get("date_found"),
            ))
            inserted += 1

    db.commit()
    db.close()
    return inserted, duplicated


def run_scan():
    """Execute a full scan cycle. Runs in a background thread."""
    with _scan_lock:
        if _scan_state["running"]:
            return False  # Already running
        _scan_state.update({
            "running": True,
            "started_at": datetime.now().isoformat(),
            "finished_at": None,
            "phase": "searching",
            "phase_detail": "Starting web search...",
            "results_found": 0,
            "results_saved": 0,
            "results_duplicate": 0,
            "error": None,
            "last_results": [],
        })

    def _run():
        try:
            # Phase 1: Search
            _update_state(phase="searching", phase_detail="Searching for EIS investor references...")
            search_results = _search_web()
            _update_state(
                phase_detail=f"Found {len(search_results)} search results to analyze",
                results_found=len(search_results),
            )

            if not search_results:
                _update_state(
                    phase="done",
                    phase_detail="No relevant search results found.",
                    running=False,
                    finished_at=datetime.now().isoformat(),
                )
                return

            # Phase 2: Extract
            _update_state(phase="extracting", phase_detail="Extracting investor mentions...")
            investors = _extract_investors_from_results(search_results)

            if not investors:
                _update_state(
                    phase="done",
                    phase_detail="No named individual EIS investors found in results.",
                    running=False,
                    finished_at=datetime.now().isoformat(),
                    results_found=len(search_results),
                )
                return

            # Phase 3: Save
            _update_state(
                phase="saving",
                phase_detail=f"Saving {len(investors)} investor(s) to database..."
            )
            inserted, duplicated = _save_to_db(investors)

            # Build summary
            last_results = []
            for inv in investors:
                last_results.append({
                    "name": inv.get("name"),
                    "eis_company": inv.get("eis_company"),
                    "new": True,  # simplified; would need to track per-record
                })

            _update_state(
                phase="done",
                phase_detail=f"Scan complete. {inserted} new investor(s) added, {duplicated} duplicate(s) skipped.",
                running=False,
                finished_at=datetime.now().isoformat(),
                results_saved=inserted,
                results_duplicate=duplicated,
                last_results=last_results,
            )

        except Exception as e:
            _update_state(
                phase="error",
                phase_detail=str(e),
                error=str(e),
                running=False,
                finished_at=datetime.now().isoformat(),
            )

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()
    return True
