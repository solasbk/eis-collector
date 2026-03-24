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
    # ── Direct EIS/SEIS mentions ──────────────────────────────────
    '"EIS investor" OR "SEIS investor" individual name invested',
    '"enterprise investment scheme" angel investor backed funded UK',
    '"EIS qualifying" investment round individual investor announcement',
    '"SEIS funding" OR "EIS funding" angel investor name UK startup',
    'EIS tax relief investor personal investment UK company',
    '"EIS relief" investor portfolio company invested UK',
    '"EIS3" OR "EIS1" compliance statement investor shares UK',
    '"advance assurance" EIS SEIS investor company funded UK',

    # ── Angel investment & seed rounds (UK focus) ─────────────────
    'UK angel investor seed round funded startup 2025 OR 2026',
    'UK angel investor seed round funded startup 2023 OR 2024',
    'UK angel investor seed round funded startup 2021 OR 2022',
    'UK angel investor seed round funded startup 2019 OR 2020',
    'UK angel investment announcement individual investor backed',
    'angel investor UK "led the round" OR "participated in" seed pre-seed',
    'UK startup seed funding announcement investor names',
    'UK early stage investor "angel round" OR "seed round" funded',
    '"angel investor" UK backed startup Series A individual',
    'UK startup "pre-seed" OR "seed" funding round individual investors named',
    'UK startup raises seed angel investor names announcement',
    'UK seed round funded individual angels participated',

    # ── Named angel networks and syndicates (UK-wide) ────────────
    'Cambridge Angels OR London Business Angels OR Archangels investor portfolio',
    'OION OR "Oxford Angel Network" investor funded startup',
    '24Haymarket OR Envestors OR "Equity Gap" investor funded',
    'Kelvin Capital OR TRICAPITAL OR "Wealth Club" angel investor funded',
    'Galvanise OR Newable OR "Wild Blue Cohort" angel investor UK',
    'GC Angels OR "Minerva Business Angel" OR "Anglia Capital" investor',
    '"Bristol Private Equity Club" OR "Dorset Business Angels" investor',
    '"Northwest Business Angels" OR "FSE Group" angel investor funded',
    'SFC Capital OR Haatch OR "Angel CoFund" investor UK startup',
    'Mercia OR Deepbridge OR Maven OR "Octopus Ventures" EIS individual investor',
    'UK angel network deal completed investor names',
    '"angel syndicate" UK investor invested startup EIS SEIS',
    '"angel investing" UK member portfolio companies invested',
    'UK "angel group" OR "angel network" members investors deals',
    'Midlands Engine OR Northern Powerhouse angel investor EIS backed',
    '"angel co-investment" UK individual investors fund',

    # ── Crowdfunding platforms (named investors) ──────────────────
    'site:seedrs.com investor funded EIS campaign',
    'site:crowdcube.com investor backed raised EIS',
    'site:syndicateroom.com investor funded EIS portfolio',
    'site:republic.com angel investor UK startup funded',
    'crowdfunding UK "lead investor" OR "angel investor" name funded',
    'UK crowdfunding campaign funded lead investor named',

    # ── VCTs and EIS funds (individual directors/investors) ──────
    'VCT investor director UK "venture capital trust" individual',
    '"EIS fund" investor portfolio UK individual name invested',
    'Baronsmead OR Mobeus OR ProVen OR Albion VCT investor director',
    'Puma Investments OR Calculus Capital OR Triple Point EIS individual investor',
    'Oxford Capital OR Deepbridge OR YFM Equity EIS investor',
    '"Par Equity" OR "Foresight Group" EIS investor individual UK',
    'Guinness Asset Management OR "Seneca Partners" EIS investor UK',

    # ── Companies House and regulatory filings ────────────────────
    '"allotment of shares" EIS investor UK',
    'UK startup "share allotment" individual investor SEIS EIS',
    'site:find-and-update.company-information.service.gov.uk "allotment" shares investor',
    '"persons with significant control" UK startup angel investor',
    'UK startup confirmation statement shareholder angel investor',

    # ── UK startup funding press (articles naming investors) ──────
    'site:uktech.news investor angel funded UK startup',
    'site:sifted.eu angel investor funded UK startup seed',
    'site:beauhurst.com angel investor UK EIS portfolio',
    'site:growthbusiness.co.uk angel investor UK startup funded',
    'site:techcrunch.com UK angel investor seed funded startup',
    'site:cityam.com angel investor UK startup funded backed',
    'site:altfi.com investor EIS SEIS funded UK fintech',
    'site:eu-startups.com UK angel investor funded startup seed',
    'site:startupmag.co.uk angel investor funded UK startup seed',
    'site:startupmag.co.uk most active UK investors angels',
    'site:techfundingnews.com UK angel investor startup funded',
    'site:businesscloud.co.uk angel investor funded UK startup',
    'site:startups.co.uk angel investor EIS funded round',
    'site:verdict.co.uk OR site:tech.eu UK startup angel investor funded',
    'site:prolificnorth.co.uk OR site:bdaily.co.uk angel investor funded startup',
    'site:uktn.co.uk OR site:maddyness.com angel investor UK seed funded',

    # ── Investor directories and databases ───────────────────────
    'site:openvc.app angel investor UK startup',
    'site:seedlegals.com investor directory angel UK',
    'site:angellist.com UK angel investor startup funded',
    'site:crunchbase.com UK angel investor seed funded individual',
    'site:pitchbook.com UK angel investor EIS seed funded',
    'site:dealroom.co UK angel investor funded startup seed',
    'site:robotmascot.co.uk angel investor network UK directory',
    'site:republic.com UK angel investor funded startup',

    # ── LinkedIn and social profiles ──────────────────────────────
    'site:linkedin.com "EIS" OR "SEIS" "angel investor" UK invested',
    'site:linkedin.com "angel investor" UK "invested in" startup portfolio',
    'site:linkedin.com "angel investor" UK "portfolio" company funded EIS',
    'site:twitter.com OR site:x.com UK angel investor invested startup',

    # ── Historical and broader date ranges ────────────────────────
    '"EIS investor" UK angel invested startup 2020 OR 2021',
    '"angel investor" UK startup seed round funded 2022 OR 2023',
    'UK angel investment round announcement names 2020 2021 2022',
    'UK "EIS" investment individual investor announcement 2019 OR 2020',
    'UK angel investor portfolio companies funded 2017 OR 2018',

    # ── Awards, lists, and directories of angel investors ─────────
    'UK "angel investor of the year" OR "angel investor award" names',
    '"top angel investors" UK list names EIS SEIS',
    '"most active angel investors" UK 2024 OR 2025 OR 2026',
    '"most active angel investors" UK 2022 OR 2023',
    'UK angel investor directory list names profiles',
    'UKBAA OR "UK Business Angels Association" investor members',
    'EISA OR "EIS Association" investor members UK',

    # ── Sector-specific angel investor queries ────────────────────
    'UK angel investor healthtech biotech life sciences funded',
    'UK angel investor fintech insurtech funded seed',
    'UK angel investor cleantech greentech climate funded',
    'UK angel investor AI machine learning deeptech funded',
    'UK angel investor proptech edtech foodtech funded seed',
    'UK angel investor SaaS software B2B funded startup',

    # ── Regional UK startup ecosystems ────────────────────────────
    'London angel investor seed funded startup names 2025 2026',
    'Manchester angel investor seed funded startup names',
    'Edinburgh OR Glasgow angel investor startup funded Scotland',
    'Cambridge OR Oxford angel investor startup funded seed',
    'Bristol OR Birmingham angel investor startup funded seed',
    'Leeds OR Sheffield OR Newcastle angel investor funded startup',
    'Northern Ireland OR Wales angel investor funded startup seed',
]

# ── Direct source URLs to browse for investor names ──────────────
# These are known pages that list investors, portfolios, or deals.
# The scanner fetches these directly (no search needed) and extracts names.
DIRECT_SOURCES = [
    # ── Angel networks — member/portfolio pages ───────────────────
    "https://www.angelsden.com/investors",
    "https://www.cambridgeangels.com/members",
    "https://www.londonbusinessangels.co.uk/our-angels",
    "https://www.archangelsinvestors.com/team",
    "https://www.gabrieltechnology.com/team",
    "https://www.midlandsengine.org/investment-fund/",
    "https://www.syndicateroom.com/investors",
    "https://www.envestors.co.uk/investors",
    "https://www.24haymarket.com/team",
    "https://www.equitygap.co.uk/team",
    "https://www.kelvincapital.com/team",
    "https://www.gcangels.co.uk/investors",
    "https://www.minerva.uk.net/angels",
    "https://www.angliacapitalgroup.co.uk/investors",
    "https://www.dorsetbusinessangels.co.uk/members",
    "https://www.nwbusinessangels.co.uk/angels",
    "https://www.bristolprivateequityclub.com/members",
    "https://www.galvanise.com/team",
    "https://www.newable.co.uk/angel-investors/",
    # ── Scottish angel networks ───────────────────────────────
    "https://www.tricapital.co.uk/team",
    "https://www.lbangels.co.uk/our-angels",
    "https://www.scottishenterprise.com/support-for-businesses/funding-and-grants/co-investment-funds",
    # ── Crowdfunding — recent funded campaigns ────────────────────
    "https://www.seedrs.com/invest/campaigns?status=funded",
    "https://www.crowdcube.com/explore/funded",
    "https://europe.republic.com/invest",
    # ── EIS/VCT fund managers — team and investor pages ───────────
    "https://www.oxfordcapital.co.uk/team/",
    "https://www.parequity.com/team/",
    "https://www.calculus.co.uk/about-us/team/",
    "https://www.pumainvestments.co.uk/about-us/team/",
    "https://www.triplepoint.co.uk/our-team/",
    "https://www.deepbridgecapital.com/team",
    "https://www.guinnessgi.com/team",
    "https://www.senecapartners.co.uk/team",
    "https://www.yfmequity.co.uk/team",
    "https://www.foresightgroup.eu/about-us/our-team",
    "https://www.albion.capital/about/our-team",
    "https://www.mercia.co.uk/team",
    "https://www.sfccapital.com/portfolio",
    "https://www.haatch.com/portfolio",
    # ── Investor directories and lists ───────────────────────────
    "https://www.beauhurst.com/research/angel-investors-uk/",
    "https://www.beauhurst.com/research/top-angel-investors/",
    "https://www.beauhurst.com/blog/top-angel-networks-uk/",
    "https://www.openvc.app/investor-lists/angel-investors-uk",
    "https://www.openvc.app/investor-lists/angel-investors-london",
    "https://seedlegals.com/resources/category/investor-directory/",
    "https://www.startupmag.co.uk/investors/angel-investors/",
    "https://www.startupmag.co.uk/market/startup-investors/",
    "https://www.robotmascot.co.uk/blog/uk-angel-investment-groups-directory/",
    "https://europe.republic.com/academy/top-22-uk-active-angel-investor-networks",
    # ── Funding news roundups (name investors in articles) ─────────
    "https://www.startupmag.co.uk/funding/",
    "https://www.uktech.news/tag/angel-investor",
    "https://sifted.eu/sector/angel-investment",
    "https://www.growthbusiness.co.uk/tag/angel-investment",
    "https://techfundingnews.com/category/uk/",
    "https://startups.co.uk/news/",
    "https://startups.co.uk/funding/angel-investors/",
    # ── Industry associations ─────────────────────────────────
    "https://www.ukbaa.org.uk/member-directory/",
    "https://ukbaa.org.uk/membership/angel-hubs/",
    "https://www.eisa.org.uk/about-eis/facts-and-figures/",
    # ── SeedLegals sector-specific investor pages ─────────────────
    "https://seedlegals.com/resources/category/investor-directory/page/2/",
]

# ── Extraction config ────────────────────────────────────────────
BATCH_SIZE = 10  # search results per LLM call (snippet mode)
PAGE_FETCH_LIMIT = 120  # max pages to fetch full content from
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
        # Keep log to last 150 entries
        if len(_scan_state["log"]) > 150:
            _scan_state["log"] = _scan_state["log"][-150:]
    print(f"[scanner] {msg}")


# ── Search ───────────────────────────────────────────────────────

def _search_web():
    """Run searches using Serper API (primary) with fallbacks, plus direct sources."""
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

    # Add direct source URLs (known investor list pages)
    direct_count = 0
    for url in DIRECT_SOURCES:
        if url not in seen_urls:
            seen_urls.add(url)
            all_results.append({
                "title": "[Direct Source] " + url.split("//")[1].split("/")[0],
                "url": url,
                "snippet": "",
                "_direct": True,  # flag for scoring boost
            })
            direct_count += 1
    _log(f"Added {direct_count} direct source URLs")

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
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY", "")
    gemini_key = os.environ.get("GEMINI_API_KEY", "")

    # Build ordered list of providers to try (Gemini primary — cheapest)
    providers = []
    if gemini_key:
        providers.append(("gemini", gemini_key))
    if anthropic_key:
        providers.append(("anthropic", anthropic_key))

    if not providers:
        _log("No LLM API key set. Set GEMINI_API_KEY or ANTHROPIC_API_KEY in Render Environment.")
        _update_state(
            phase="done",
            phase_detail=f"Found {len(results)} search results but no LLM API key configured.",
        )
        return []

    primary_name, _ = providers[0]
    fallback_name = providers[1][0] if len(providers) > 1 else None
    _log(f"Primary LLM: {primary_name}" + (f", fallback: {fallback_name}" if fallback_name else ""))

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

            # Try each provider in order until one succeeds
            investor_data = None
            for prov_name, prov_key in providers:
                try:
                    investor_data = _extract_from_page(prov_name, prov_key, result, page_text)
                    consecutive_errors = 0
                    break  # success — stop trying providers
                except Exception as llm_err:
                    err_str = str(llm_err)
                    _log(f"Page {i+1} {prov_name} failed: {err_str[:120]}")
                    if "429" in err_str:
                        wait = min(20 * (consecutive_errors + 1), 60)
                        _log(f"{prov_name} rate limited. Waiting {wait}s before trying next provider...")
                        time.sleep(wait)
                    # Continue to next provider

            if investor_data is None:
                # All providers failed for this page
                consecutive_errors += 1
                _log(f"Page {i+1}: all providers failed ({consecutive_errors} consecutive)")
                if consecutive_errors >= 5:
                    _log(f"Aborting after {consecutive_errors} consecutive all-provider failures.")
                    break
            else:
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
                else:
                    _log(f"Page {i+1}: no investors found on this page")

        except Exception as e:
            consecutive_errors += 1
            err_str = str(e)
            _log(f"Page {i+1} fetch error: {err_str[:150]}")
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

        # Direct sources get high priority (known investor list pages)
        if r.get("_direct"):
            score += 10

        # High-value signals
        if any(w in text for w in ["angel investor", "angel round", "seed round", "backed by", "invested in"]):
            score += 3
        if any(w in text for w in ["eis", "seis", "enterprise investment scheme"]):
            score += 3
        if any(w in text for w in ["announced", "raises", "funding round", "secures"]):
            score += 2
        if any(w in text for w in ["individual", "personally invested", "angel network"]):
            score += 2
        if any(w in text for w in ["portfolio", "our investors", "member", "backed companies"]):
            score += 2

        # Source quality signals
        url = r.get("url", "").lower()
        if any(d in url for d in ["techcrunch", "sifted", "uktech.news", "cityam", "growthbusiness"]):
            score += 2
        if any(d in url for d in ["seedrs.com", "crowdcube.com", "beauhurst.com", "syndicateroom.com"]):
            score += 3
        if any(d in url for d in ["crunchbase.com", "pitchbook.com", "dealroom.co", "openvc.app"]):
            score += 2
        if any(d in url for d in ["linkedin.com", "companieshouse", "company-information.service.gov.uk"]):
            score += 1
        if any(d in url for d in ["angelsden.com", "cambridgeangels", "londonbusinessangels", "ukbaa.org"]):
            score += 3
        if any(d in url for d in ["startupmag.co.uk", "techfundingnews.com", "seedlegals.com", "republic.com"]):
            score += 2
        if any(d in url for d in ["envestors.co", "equitygap.co", "kelvincapital", "24haymarket", "galvanise.com"]):
            score += 3
        if any(d in url for d in ["sfccapital.com", "haatch.com", "parequity.com", "robotmascot.co.uk"]):
            score += 2
        if any(d in url for d in ["eisa.org.uk", "mercia.co", "deepbridge", "foresightgroup", "albion.capital"]):
            score += 2

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
    """Call Gemini 2.5 Flash API."""
    with httpx.Client(timeout=60) as client:
        resp = client.post(
            "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent",
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
