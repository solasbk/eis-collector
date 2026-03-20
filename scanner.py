"""scanner.py -- EIS Investor web scanner.

Searches for public references to individuals investing
in UK EIS/SEIS qualifying companies, then uses an LLM to extract
structured investor records.
"""

import json
import os
import threading
import time
import sqlite3
import re
from datetime import datetime, date
from typing import Optional

import httpx

DATA_DIR = os.environ.get("DATA_DIR", ".")
DB_PATH = os.path.join(DATA_DIR, "eis_investors.db")

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
    "log": [],                  # diagnostic log for debugging
}

SEARCH_QUERIES = [
    # Direct EIS/SEIS mentions
    '"EIS investor" OR "SEIS investor" individual name invested 2024 OR 2025 OR 2026',
    '"enterprise investment scheme" angel investor backed funded UK',
    '"EIS qualifying" investment round individual investor announcement',
    '"SEIS funding" OR "EIS funding" angel investor name UK startup',
    'EIS tax relief investor personal investment UK company',

    # Angel investment & seed rounds (UK focus — many are EIS-qualifying)
    'UK angel investor seed round funded startup 2025 OR 2026',
    'UK angel investment announcement individual investor backed',
    'angel investor UK "led the round" OR "participated in" seed pre-seed',
    'UK startup seed funding announcement investor names 2025',
    'UK early stage investor "angel round" OR "seed round" funded',

    # EIS fund managers and networks — they list investors/deals
    'site:seedrs.com OR site:crowdcube.com investor funded EIS',
    'site:linkedin.com "EIS" OR "SEIS" "angel investor" UK invested',
    'Mercia OR Deepbridge OR Maven OR "Octopus Ventures" EIS investment individual investor',
    'UK angel network deal completed investor names 2025 OR 2026',
    '"angel syndicate" UK investor invested startup EIS SEIS',

    # Companies House and regulatory filings
    '"allotment of shares" EIS investor UK 2025 OR 2026',
    'UK startup "share allotment" individual investor SEIS EIS',

    # Industry press and directories
    'site:uktech.news OR site:sifted.eu investor angel funded UK startup',
    'site:beauhurst.com OR site:growthbusiness.co.uk angel investor UK EIS',
    '"angel investor" UK profile invested EIS qualifying companies portfolio',
]

# ── Extraction config ────────────────────────────────────────────
BATCH_SIZE = 10  # search results per LLM call (snippet mode)
PAGE_FETCH_LIMIT = 30  # max pages to fetch full content from
PAGE_MAX_CHARS = 8000  # max chars to send from each page




def get_scan_status():
    """Return current scan state (thread-safe)."""
    with _scan_lock:
        return dict(_scan_state)


def _update_state(**kwargs):
    with _scan_lock:
        _scan_state.update(kwargs)


def _log(msg):
    """Append to the diagnostic log."""
    with _scan_lock:
        _scan_state["log"].append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
        # Keep log to last 80 entries
        if len(_scan_state["log"]) > 80:
            _scan_state["log"] = _scan_state["log"][-80:]
    print(f"[scanner] {msg}")


# ── Search ───────────────────────────────────────────────────────

def _search_web():
    """Run searches using Serper API (primary) with fallbacks."""
    all_results = []
    seen_urls = set()

    serper_key = os.environ.get("SERPER_API_KEY", "")

    if serper_key:
        _log("SERPER_API_KEY found. Using Serper Google Search API.")
        serper_count = _search_serper(all_results, seen_urls, serper_key)
        _log(f"Serper returned {serper_count} results")
    else:
        _log("SERPER_API_KEY not set. Falling back to direct search (may be blocked on cloud servers).")
        ddg_count = _search_duckduckgo(all_results, seen_urls)
        _log(f"DuckDuckGo returned {ddg_count} results")

    _log(f"Total unique search results: {len(all_results)}")
    return all_results


def _search_serper(all_results, seen_urls, api_key):
    """Search via Serper.dev Google Search API."""
    count = 0

    for query in SEARCH_QUERIES:
        _update_state(phase_detail=f"Searching Google (API): {query[:50]}...")
        try:
            resp = httpx.post(
                "https://google.serper.dev/search",
                headers={
                    "X-API-KEY": api_key,
                    "Content-Type": "application/json",
                },
                json={
                    "q": query,
                    "gl": "uk",
                    "hl": "en",
                    "num": 20,
                },
                timeout=15,
            )
            _log(f"Serper status {resp.status_code} for: {query[:40]}")

            if resp.status_code == 200:
                data = resp.json()
                organic = data.get("organic", [])
                for r in organic:
                    url = r.get("link", "")
                    if url and url not in seen_urls:
                        seen_urls.add(url)
                        all_results.append({
                            "title": r.get("title", ""),
                            "url": url,
                            "snippet": r.get("snippet", ""),
                        })
                        count += 1
                _log(f"  -> {len(organic)} organic results from this query")
            elif resp.status_code == 401:
                _log("Serper API key is invalid (401). Check SERPER_API_KEY.")
                break
            elif resp.status_code == 429:
                _log("Serper rate limit hit (429). Waiting...")
                time.sleep(5)
            else:
                _log(f"Serper unexpected status {resp.status_code}: {resp.text[:200]}")
        except Exception as e:
            _log(f"Serper error: {e}")
        time.sleep(0.3)

    return count


def _search_duckduckgo(all_results, seen_urls):
    """Search via DuckDuckGo (fallback if no Serper key)."""
    count = 0
    try:
        from duckduckgo_search import DDGS
        with DDGS() as ddgs:
            for query in SEARCH_QUERIES[:3]:
                _update_state(phase_detail=f"Searching DDG: {query[:50]}...")
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
                            count += 1
                except Exception as e:
                    _log(f"DDG query error: {e}")
                time.sleep(0.5)
    except ImportError:
        _log("duckduckgo_search not installed, skipping DDG")
    except Exception as e:
        _log(f"DDG init error: {e}")
    return count


# ── Extraction ───────────────────────────────────────────────────

def _extract_investors_from_results(results):
    """Extract investor mentions by fetching page content and analyzing with LLM."""
    gemini_key = os.environ.get("GEMINI_API_KEY", "")
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")

    if gemini_key:
        _log("Using Gemini 2.0 Flash for extraction.")
        provider = "gemini"
        api_key = gemini_key
    elif anthropic_key:
        _log("Using Anthropic Claude Haiku for extraction.")
        provider = "anthropic"
        api_key = anthropic_key
    else:
        _log("No LLM API key set. Set GEMINI_API_KEY or ANTHROPIC_API_KEY in Render Environment.")
        _update_state(
            phase="done",
            phase_detail=f"Found {len(results)} search results but no LLM API key configured.",
        )
        return []

    all_investors = []
    today = date.today().isoformat()

    # Step 1: Score and rank results — prioritise pages likely to contain investor names
    scored = _score_results(results)
    top_results = scored[:PAGE_FETCH_LIMIT]
    _log(f"Scored {len(results)} results. Fetching content from top {len(top_results)} pages.")

    # Step 2: Fetch actual page content for top results
    consecutive_errors = 0
    pages_fetched = 0

    for i, result in enumerate(top_results):
        _update_state(
            phase="extracting",
            phase_detail=f"Fetching & analyzing page {i + 1}/{len(top_results)}: {result['title'][:50]}..."
        )

        try:
            # Fetch page content
            page_text = _fetch_page_text(result["url"])
            if not page_text or len(page_text.strip()) < 100:
                _log(f"Page {i+1}: too little content from {result['url'][:60]}")
                continue

            pages_fetched += 1
            _log(f"Page {i+1}: fetched {len(page_text)} chars from {result['url'][:60]}")

            # Extract investors from page content
            investor_data = _extract_from_page(provider, api_key, result, page_text)
            consecutive_errors = 0

            if investor_data:
                _log(f"Page {i+1}: found {len(investor_data)} investor(s)")
                for inv in investor_data:
                    inv.setdefault("source_url", result["url"])
                    inv["source_type"] = _classify_source(result["url"])
                    inv["source_name"] = _extract_source_name(result["url"], result["title"])
                    inv["date_found"] = today
                    inv["linkedin_url"] = None
                    all_investors.append(inv)

        except Exception as e:
            consecutive_errors += 1
            err_str = str(e)
            _log(f"Page {i+1} error: {err_str[:150]}")

            if "429" in err_str:
                wait = min(30 * consecutive_errors, 120)
                _log(f"Rate limited. Waiting {wait}s...")
                time.sleep(wait)

            if consecutive_errors >= 5:
                _log(f"Aborting after {consecutive_errors} consecutive errors.")
                break

        time.sleep(0.5)

    _log(f"Fetched {pages_fetched} pages. Found {len(all_investors)} total investor mentions.")
    return all_investors


def _score_results(results):
    """Score search results by likelihood of containing named individual investors."""
    scored = []
    for r in results:
        score = 0
        text = (r.get("title", "") + " " + r.get("snippet", "")).lower()

        # High-value signals
        if any(w in text for w in ["angel investor", "angel round", "seed round", "backed by", "invested in"]):
            score += 3
        if any(w in text for w in ["eis", "seis", "enterprise investment scheme"]):
            score += 3
        if any(w in text for w in ["announced", "raises", "funding round", "secures"]):
            score += 2
        if any(w in text for w in ["individual", "personally invested", "angel network"]):
            score += 2

        # Source quality signals
        url = r.get("url", "").lower()
        if any(d in url for d in ["techcrunch", "sifted", "uktech.news", "cityam", "growthbusiness"]):
            score += 2
        if any(d in url for d in ["seedrs.com", "crowdcube.com", "beauhurst.com"]):
            score += 2
        if any(d in url for d in ["linkedin.com", "companieshouse"]):
            score += 1

        # Penalise generic/educational content
        if any(w in text for w in ["how to invest", "guide", "what is eis", "tax relief explained"]):
            score -= 3
        if any(w in text for w in ["compare eis funds", "eis fund manager", "wealth club"]):
            score -= 2

        r["_score"] = score
        scored.append(r)

    scored.sort(key=lambda x: x["_score"], reverse=True)
    return scored


def _fetch_page_text(url):
    """Fetch a page and extract readable text content."""
    try:
        resp = httpx.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml",
            },
            timeout=15,
            follow_redirects=True,
        )
        if resp.status_code != 200:
            return None

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove script and style elements
        for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
            tag.decompose()

        text = soup.get_text(separator="\n", strip=True)

        # Truncate to limit
        if len(text) > PAGE_MAX_CHARS:
            text = text[:PAGE_MAX_CHARS]

        return text
    except Exception:
        return None


PAGE_EXTRACTION_PROMPT = """You are an analyst extracting individual investors in UK startups, particularly those using the EIS (Enterprise Investment Scheme) or SEIS (Seed Enterprise Investment Scheme).

Analyze this web page content and extract every NAMED INDIVIDUAL mentioned as having personally invested in, backed, or funded a UK startup or early-stage company.

Page title: {title}
Page URL: {url}

Page content:
{page_text}

Rules:
- Extract NAMED INDIVIDUALS (first and last name) described as investing, backing, or funding a company
- INCLUDE angel investors, seed investors, individual backers, crowdfunding investors mentioned by name
- INCLUDE people who invested in UK startups even if "EIS" is not explicitly mentioned
- EXCLUDE fund managers or VCs only mentioned as managing a fund (unless they also made a personal investment)
- EXCLUDE company names without an associated individual name
- EXCLUDE generic mentions like "investors" without specific names
- Extract as many qualifying individuals as you can find on the page

Return a JSON object:
{{"investors": [
  {{
    "name": "Full Name",
    "role": "Their role/title (or 'Angel Investor' if unknown)",
    "company": "Their employer/firm (or 'Independent' if unknown)",
    "eis_company": "The company they invested in",
    "sector": "The invested company's sector (brief)",
    "amount": "Investment amount if disclosed, otherwise 'Undisclosed'",
    "context_quote": "Brief quote from the page showing the investment"
  }}
]}}

If no qualifying investors found, return: {{"investors": []}}
Return ONLY valid JSON."""


def _extract_from_page(provider, api_key, result, page_text):
    """Extract investors from a full page's text content."""
    prompt = PAGE_EXTRACTION_PROMPT.format(
        title=result["title"],
        url=result["url"],
        page_text=page_text,
    )

    if provider == "gemini":
        return _call_gemini(api_key, prompt)
    else:
        return _call_anthropic(api_key, prompt)


def _call_gemini(api_key, prompt):
    """Call Gemini 2.0 Flash API."""
    with httpx.Client(timeout=60) as client:
        resp = client.post(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent",
            headers={
                "x-goog-api-key": api_key,
                "Content-Type": "application/json",
            },
            json={
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "temperature": 0.1,
                    "maxOutputTokens": 4096,
                    "responseMimeType": "application/json",
                },
            },
        )
        resp.raise_for_status()
        data = resp.json()
        content = data["candidates"][0]["content"]["parts"][0]["text"]
        return _parse_investor_json(content)


def _call_anthropic(api_key, prompt):
    """Call Anthropic Claude Haiku API."""
    with httpx.Client(timeout=60) as client:
        resp = client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 4096,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        resp.raise_for_status()
        content = resp.json()["content"][0]["text"]
        return _parse_investor_json(content)


def _parse_investor_json(content):
    """Parse investor JSON from LLM response text."""
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


# ── Helpers ──────────────────────────────────────────────────────

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


# ── Main scan entry point ────────────────────────────────────────

def run_scan():
    """Execute a full scan cycle. Runs in a background thread."""
    with _scan_lock:
        if _scan_state["running"]:
            return False
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
            "log": [],
        })

    def _run():
        try:
            # Phase 1: Search
            _update_state(phase="searching", phase_detail="Searching for EIS investor references...")
            _log("Scan started")
            _log(f"GEMINI_API_KEY set: {'yes' if os.environ.get('GEMINI_API_KEY') else 'NO'}")
            _log(f"ANTHROPIC_API_KEY set: {'yes' if os.environ.get('ANTHROPIC_API_KEY') else 'NO'}")
            _log(f"SERPER_API_KEY set: {'yes' if os.environ.get('SERPER_API_KEY') else 'NO'}")

            search_results = _search_web()
            _update_state(results_found=len(search_results))

            if not search_results:
                _log("No search results from any engine.")
                _update_state(
                    phase="done",
                    phase_detail="Web search returned no results.",
                    running=False,
                    finished_at=datetime.now().isoformat(),
                )
                return

            # Phase 2: Extract
            _update_state(phase="extracting", phase_detail="Extracting investor mentions...")
            investors = _extract_investors_from_results(search_results)

            if not investors:
                detail = f"Analyzed {len(search_results)} results. No named individual EIS investors found."
                _log(detail)
                _update_state(
                    phase="done",
                    phase_detail=detail,
                    running=False,
                    finished_at=datetime.now().isoformat(),
                )
                return

            # Phase 3: Save
            _update_state(
                phase="saving",
                phase_detail=f"Saving {len(investors)} investor(s) to database..."
            )
            inserted, duplicated = _save_to_db(investors)
            _log(f"Saved: {inserted} new, {duplicated} duplicates")

            last_results = [
                {"name": inv.get("name"), "eis_company": inv.get("eis_company")}
                for inv in investors
            ]

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
            _log(f"Scan error: {e}")
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
