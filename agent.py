import asyncio
import json
import logging
import os
import re
import sys
import threading
import time
import traceback
import warnings
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

load_dotenv()

logging.basicConfig(level=logging.INFO, format="[ARA] %(levelname)s %(message)s")
log = logging.getLogger("ara")

# ── Suppress noisy GCP / ADK / genai internal logging ──────────────────────
# ERROR (not WARNING) because ADK's own log.warning() calls — e.g. "there are
# non-text parts in the response" — are emitted AT warning level, so setting
# the parent logger to WARNING does not hide them; only ERROR+ does.
for _noisy in ("google", "google.adk", "google.genai", "google_genai",
               "urllib3", "httpx", "grpc"):
    logging.getLogger(_noisy).setLevel(logging.ERROR)

# ── Suppress Python UserWarnings raised by ADK (e.g. the experimental
#    PROGRESSIVE_SSE_STREAMING feature notice) — these come through the
#    `warnings` module, not `logging`, so they need a separate filter.
warnings.filterwarnings("ignore", category=UserWarning, module="google.*")
warnings.filterwarnings("ignore", message=".*EXPERIMENTAL.*")
warnings.filterwarnings("ignore", message=".*non-text parts.*")

_SA_FILE = Path(__file__).parent / "serviceaccount.json"
if _SA_FILE.exists():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(_SA_FILE)

os.environ.setdefault("GOOGLE_GENAI_USE_VERTEXAI", "true")
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", "us-central1")

SERPER_API_KEY    = os.getenv("SERPER_API_KEY",    "")
TAVILY_API_KEY    = os.getenv("TAVILY_API_KEY",    "")
RAPIDAPI_KEY      = os.getenv("RAPIDAPI_KEY",      "")
FMP_API_KEY       = os.getenv("FMP_API_KEY",       "")
APIFY_API_TOKEN   = os.getenv("APIFY_API_TOKEN",   "")
SCRAPFLY_API_KEY  = os.getenv("SCRAPFLY_API_KEY",  "")
EXPLORIUM_API_KEY = os.getenv("EXPLORIUM_API_KEY", "")
SEC_API_KEY       = os.getenv("SEC_API_KEY",       "")

_GEM_ACTOR = "krawlify~gem-portal-scraper"  # Apify URL-path actor IDs use '~' not '/'
_UA = {"User-Agent": "AccountResearchAgent/1.0 abhishekabhi944677@gmail.com"}
# Per-request tool status list (reset before each run)
_tool_statuses: list = []


# ══════════════════════════════════════════════════════════════════════════════
#  SHARED HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _get(url: str, **kw) -> requests.Response:
    kw.setdefault("timeout", 15)
    kw.setdefault("headers", _UA)
    r = requests.get(url, **kw)
    r.raise_for_status()
    return r


def _post(url: str, **kw) -> requests.Response:
    kw.setdefault("timeout", 15)
    kw.setdefault("headers", _UA)
    r = requests.post(url, **kw)
    r.raise_for_status()
    return r


def _trunc(text: str, n: int) -> str:
    t = (text or "").strip()
    return t[:n] + "…" if len(t) > n else t


def _item(title="", summary="", url="", date="") -> dict:
    return {
        "title":   _trunc(title, 120),
        "summary": _trunc(summary, 300),
        "url":     url or "",
        "date":    date or "",
    }


def _ok(source: str, items: list) -> dict:
    status = "ok" if items else "empty"
    log.info("  ✓ %-28s [%s] — %d item(s)", source, status, len(items))
    rec = {
        "source":     source,
        "status":     status,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "item_count": len(items),
        "items":      items,      # kept — stripped at Flask layer for brief, but sent for hyperlinks
        "error":      None,
    }
    _tool_statuses.append(rec)
    return rec


def _err(source: str, msg: str) -> dict:
    log.warning("  ✗ %-28s [error] — %s", source, msg)
    rec = {
        "source":     source,
        "status":     "error",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "item_count": 0,
        "items":      [],
        "error":      str(msg),
    }
    _tool_statuses.append(rec)
    return rec


def _empty(source: str, reason: str) -> dict:
    """
    Use when a tool ran successfully but genuinely found no data
    (e.g. private company with no SEC filings, no registry match).
    This is NOT a failure — the API/scrape worked, there's just nothing
    to report — so it must not be tagged 'error' in the UI.
    """
    log.info("  ○ %-28s [empty] — %s", source, reason)
    rec = {
        "source":     source,
        "status":     "empty",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "item_count": 0,
        "items":      [],
        "error":      None,
        "note":       str(reason),
    }
    _tool_statuses.append(rec)
    return rec


def _scrape(url: str) -> str:
    """Fetch a URL and return visible text (up to 1 200 chars).
    Returns '' for any non-200 response so 404/403/5xx error pages are
    never mistaken for real content and handed to the model as evidence."""
    for scheme in ("https", "http"):
        target = re.sub(r"^https?://", f"{scheme}://", url)
        try:
            r = requests.get(target, timeout=12, headers=_UA)
            if r.status_code != 200:
                continue          # try the other scheme, then give up — no fake content
            soup = BeautifulSoup(r.text, "lxml")
            meta = soup.find("meta", attrs={"name": re.compile(r"description", re.I)})
            meta_desc = (meta.get("content") or "").strip()[:300] if meta else ""
            for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
                tag.decompose()
            body = " ".join(soup.get_text(" ", strip=True).split())[:1200]
            if meta_desc and len(body) < 300:
                return f"{meta_desc} {body}".strip()[:1200]
            return body
        except Exception:
            continue
    return ""


def _scrapfly(url: str, render_js: bool = True, use_proxy_pool: bool = True) -> str:
    if not SCRAPFLY_API_KEY:
        return _scrape(url)

    # We iterate through asp=true then asp=false to try and bypass bot-walls.
    # Increased timeout to 45s to handle slow residential proxies better.
    for asp in ("true", "false"):
        try:
            params = {
                "key":       SCRAPFLY_API_KEY,
                "url":       url,
                "asp":       asp,
                "render_js": "true" if render_js else "false",
                "format":    "markdown",
                "country":   "us",
            }
            if use_proxy_pool:
                params["proxy_pool"] = "public_residential_pool"

            # Increased timeout from 30 to 45 to reduce ReadTimeout errors
            r = requests.get("https://api.scrapfly.io/scrape", params=params, timeout=45)

            if r.status_code == 400:
                try:
                    detail = r.json()
                except Exception:
                    detail = r.text[:300]
                log.info("Scrapfly 400 asp=%s for %s → %s", asp, url, detail)
                continue

            r.raise_for_status()
            result = r.json().get("result", {})
            origin_status = result.get("status_code")
            content = (result.get("content") or "").strip()

            # If the origin site (Crunchbase/G2) returns 403 even via Scrapfly,
            # we log it and try the next ASP setting or fallback.
            if origin_status and origin_status >= 400:
                log.info("Scrapfly reached %s but origin returned %s (ASP: %s)", url, origin_status, asp)
                continue
                
            if content:
                return content[:1200]
            log.info("Scrapfly asp=%s returned empty content for %s", asp, url)
        except requests.exceptions.Timeout:
            log.info("Scrapfly timeout (45s) for %s with asp=%s", url, asp)
            continue
        except Exception as exc:
            log.info("Scrapfly asp=%s exception for %s: %s", asp, url, exc)
            continue

    # Final fallback to standard requests if Scrapfly fails
    return _scrape(url)


# Broader bot-wall / gate detection — catches Cloudflare's "Just a moment",
# Crunchbase's signup gate, and generic verification prompts that a 200
# response can still contain.
_GARBAGE_SIGNALS = (
    "verify you are human", "please solve this captcha", "haproxy challenge",
    "enable cookies", "essential cookies", "analytics cookies", "cookie preferences",
    "please enable js", "you have been blocked", "cloudflare",
    "no results found", "no matching companies", "access denied", "403 forbidden",
    "please turn javascript on", "just a moment", "checking your browser",
    "sign up to see more", "sign up for free", "log in to continue",
    "verifying you are human", "ray id",
)


def _is_useful(text: str, min_len: int = 150) -> bool:
    """Return False if text is bot-wall / cookie-consent / empty shell."""
    if not text or len(text) < min_len:
        return False
    lower = text.lower()
    return not any(sig in lower for sig in _GARBAGE_SIGNALS)

# ══════════════════════════════════════════════════════════════════════════════
#  TOOL FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_company_website(domain: str) -> dict:
    """
    Scrape homepage, /about, /careers, /blog. Call this first — it anchors synthesis.
    Args:
        domain: Bare domain e.g. 'stripe.com'
    """
    if not domain:
        return _err("company_website", "No domain provided")
    domain = re.sub(r"^https?://", "", domain).rstrip("/")
    items  = []
    for path in ["", "/about", "/careers", "/blog"]:
        text = _scrape(f"https://{domain}{path}")
        if text:
            items.append(_item(
                title   = f"{domain} — {path.strip('/') or 'homepage'}",
                summary = text,
                url     = f"https://{domain}{path}",
            ))
    return _ok("company_website", items) if items else _err("company_website", f"Could not scrape {domain}")


def search_company_news(company_name: str) -> dict:
    """
    Fetch recent news via Serper (primary) and Tavily (fallback). Prioritise last 90 days.
    Args:
        company_name: Full company name e.g. 'Stripe Inc'
    """
    if not company_name:
        return _err("news", "No company name provided")

    if SERPER_API_KEY:
        try:
            r = _post(
                "https://google.serper.dev/news",
                json    = {"q": company_name, "num": 10},
                headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            )
            items = [
                _item(title=n.get("title",""), summary=n.get("snippet",""), url=n.get("link",""), date=n.get("date",""))
                for n in r.json().get("news", [])[:10]
            ]
            if items:
                return _ok("news", items)
        except Exception as exc:
            log.warning("Serper news failed: %s", exc)

    if TAVILY_API_KEY:
        try:
            r = _post(
                "https://api.tavily.com/search",
                json    = {"api_key": TAVILY_API_KEY, "query": f"{company_name} news", "search_depth": "basic", "max_results": 10},
                headers = {"Content-Type": "application/json"},
            )
            items = [
                _item(title=x.get("title",""), summary=x.get("content",""), url=x.get("url",""), date=x.get("published_date",""))
                for x in r.json().get("results", [])[:10]
            ]
            return _ok("news", items)
        except Exception as exc:
            return _err("news", str(exc))

    return _err("news", "SERPER_API_KEY and TAVILY_API_KEY both unset")


def fetch_job_boards(company_name: str, domain: str = "") -> dict:
    """
    Fetch open jobs from Greenhouse (primary), JSearch/RapidAPI (secondary),
    then Serper job search (tertiary), and finally direct careers-page scrape.
    Args:
        company_name: Company name e.g. 'Stripe'
        domain:       Company domain e.g. 'stripe.com'
    """
    if not company_name:
        return _err("job_boards", "No company name provided")

    items = []

    # ── 1. Greenhouse (exact slug, then cleaned slug) ──────────────────────────
    def _greenhouse(slug: str) -> list:
        try:
            # We use requests.get directly or modify _get to handle 404s. 
            # Since _get calls raise_for_status(), we'll catch the HTTPError 
            # specifically to avoid logging 404s as "failures".
            url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
            r = requests.get(url, headers=_UA, timeout=10)
            
            if r.status_code == 404:
                return [] # Quietly return if company doesn't use Greenhouse
                
            r.raise_for_status()
            jobs = r.json().get("jobs", [])
            result = []
            for job in jobs[:12]:
                dept = (job.get("departments") or [{}])[0].get("name", "N/A")
                loc  = job.get("location", {}).get("name", "N/A")
                result.append(_item(
                    title   = job.get("title", ""),
                    summary = f"Dept: {dept} | Location: {loc}",
                    url     = job.get("absolute_url", ""),
                    date    = (job.get("updated_at", "") or "")[:10],
                ))
            return result
        except Exception as exc:
            # Only log actual errors (timeouts, 500s), not 404s
            if not (isinstance(exc, requests.exceptions.HTTPError) and exc.response.status_code == 404):
                log.info("Greenhouse lookup error for slug=%s: %s", slug, exc)
            return []

    # Try multiple slug variants
    name_slug   = re.sub(r"[^a-z0-9\-]", "", company_name.lower().replace(" ", "-"))
    _domain_clean = re.sub(r"^www\.", "", domain or "")
    domain_slug = re.sub(r"\.[^.]+$", "", _domain_clean.split(".")[0])
    
    # Deduplicate and filter empty slugs
    potential_slugs = [s for s in dict.fromkeys([name_slug, domain_slug]) if s]
    
    for slug in potential_slugs:
        items = _greenhouse(slug)
        if items:
            break

    # ── 2. JSearch / RapidAPI ─────────────────────────────────────────────────
    if RAPIDAPI_KEY and len(items) < 6:
        try:
            r = _get(
                "https://jsearch.p.rapidapi.com/search",
                params  = {"query": f"{company_name} jobs", "num_pages": "1"},
                headers = {"X-RapidAPI-Key": RAPIDAPI_KEY, "X-RapidAPI-Host": "jsearch.p.rapidapi.com"},
            )
            for job in r.json().get("data", [])[:10]:
                items.append(_item(
                    title   = job.get("job_title", ""),
                    summary = f"Type: {job.get('job_employment_type','N/A')} | {_trunc(job.get('job_description',''), 200)}",
                    url     = job.get("job_apply_link", ""),
                    date    = (job.get("job_posted_at_datetime_utc", "") or "")[:10],
                ))
        except Exception as exc:
            log.info("JSearch failed: %s", exc)

    # ── 3. Serper job search ───────────────────────────────────────────────────
    if SERPER_API_KEY and len(items) < 4:
        try:
            r = _post(
                "https://google.serper.dev/search",
                json    = {"q": f'site:{domain} jobs OR careers' if domain else f'"{company_name}" jobs site:linkedin.com OR site:indeed.com', "num": 8},
                headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            )
            for x in r.json().get("organic", [])[:8]:
                link = x.get("link", "")
                items.append(_item(
                    title   = x.get("title", ""),
                    summary = x.get("snippet", ""),
                    url     = link,
                ))
        except Exception as exc:
            log.info("Serper jobs search failed: %s", exc)

    # ── 4. Direct careers-page scrape (always run if domain provided) ──────────
    if domain:
        careers_urls = [
            f"https://{domain}/career",
            f"https://{domain}/careers",
            f"https://{domain}/jobs",
            f"https://{domain}/join",
            f"https://{domain}/careers/open-positions",
            f"https://{domain}/about/careers",
            f"https://{domain}/about/career",
            f"https://{domain}/company/careers",
        ]
        JOB_KEYWORDS = (
            "hiring", "openings", "open position", "job description",
            "apply", "vacanc", "we're looking", "we are looking", "join our team",
        )

        best_text, best_url, best_score = "", "", 0
        for curl in careers_urls:
            text = _scrape(curl)
            if not text or len(text) <= 80:
                continue
            lower = text.lower()
            keyword_hits = sum(1 for kw in JOB_KEYWORDS if kw in lower)
            score = len(text) + (keyword_hits * 500)
            if score > best_score:
                best_text, best_url, best_score = text, curl, score

        if best_text:
            items.append(_item(
                title   = f"{company_name} — Careers Page",
                summary = _trunc(best_text, 300),
                url     = best_url,
            ))
            log.info("Careers page selected: %s (score=%d)", best_url, best_score)

    return _ok("job_boards", items) if items else _empty("job_boards", "No open roles found via any source")


def fetch_financial_filings(company_name: str) -> dict:
    """
    Retrieve SEC filings and financial metrics. Call only for likely US-listed public companies.
    Priority: sec-api.io Query API → FMP key metrics → free SEC EDGAR submissions.
    Args:
        company_name: Company name e.g. 'Apple Inc'
    """
    if not company_name:
        return _err("financial_filings", "No company name")

    items = []

    if SEC_API_KEY:
        try:
            query = (
                f'companyName:"{company_name}" AND '
                f'(formType:"10-K" OR formType:"10-Q" OR formType:"8-K")'
            )
            r = _post(
                "https://api.sec-api.io",
                json    = {"query": query, "from": "0", "size": "10",
                           "sort": [{"filedAt": {"order": "desc"}}]},
                headers = {"Authorization": SEC_API_KEY, "Content-Type": "application/json"},
            )
            for f in r.json().get("filings", [])[:10]:
                items.append(_item(
                    title   = f"{f.get('formType','SEC')} — {f.get('companyName', company_name)}",
                    summary = (f"Filed: {(f.get('filedAt',''))[:10]} | "
                               f"Period: {f.get('periodOfReport','N/A')} | "
                               f"Ticker: {f.get('ticker','N/A')} | "
                               f"CIK: {f.get('cik','N/A')}"),
                    url     = f.get("linkToFilingDetails", ""),
                    date    = (f.get("filedAt", ""))[:10],
                ))
            if items:
                log.info("sec-api.io returned %d filings", len(items))
        except Exception as exc:
            log.info("sec-api.io failed: %s", exc)

    if FMP_API_KEY and len(items) < 6:
        try:
            r_s = _get("https://financialmodelingprep.com/stable/search-name",
                       params={"query": company_name, "limit": 3, "apikey": FMP_API_KEY})
            results = r_s.json()
            if results:
                ticker = results[0].get("symbol", "")
                r_m = _get("https://financialmodelingprep.com/stable/profile",
                    params={"symbol": ticker, "apikey": FMP_API_KEY})
                profiles = r_m.json()
                profile_list = profiles if isinstance(profiles, list) else [profiles]
                for p in profile_list[:1]:
                    items.append(_item(
                        title   = f"FMP Profile — {p.get('companyName', ticker)} ({ticker})",
                        summary = (f"MarketCap: {p.get('mktCap','N/A')} | "
                                    f"Price: {p.get('price','N/A')} {p.get('currency','')} | "
                                    f"Employees: {p.get('fullTimeEmployees','N/A')} | "
                                    f"Sector: {p.get('sector','N/A')} | "
                                    f"Industry: {p.get('industry','N/A')} | "
                                    f"Country: {p.get('country','N/A')}"),
                        url     = f"https://financialmodelingprep.com/financial-statements/{ticker}",
                        date    = p.get("ipoDate", ""),
                    ))
        except Exception as exc:
            log.info("FMP failed: %s", exc)
    if len(items) < 4:
        try:
            r   = _get("https://www.sec.gov/files/company_tickers.json")
            nl  = company_name.lower()
            cik = next(
                (str(v["cik_str"]).zfill(10)
                 for v in r.json().values()
                 if nl in v.get("title", "").lower()),
                None,
            )
            if cik:
                r2  = _get(f"https://data.sec.gov/submissions/CIK{cik}.json")
                rec = r2.json().get("filings", {}).get("recent", {})
                for form, date, acc, doc in zip(
                    rec.get("form", []), rec.get("filingDate", []),
                    rec.get("accessionNumber", []), rec.get("primaryDocument", [])
                ):
                    if form in {"10-K", "10-Q", "8-K"}:
                        acc_c = acc.replace("-", "")
                        items.append(_item(
                            title   = f"SEC {form} — {company_name}",
                            summary = f"Filing date: {date} | Accession: {acc}",
                            url     = (f"https://www.sec.gov/Archives/edgar/data/"
                                       f"{int(cik)}/{acc_c}/{doc}"),
                            date    = date,
                        ))
                    if len(items) >= 12:
                        break
        except Exception as exc:
            log.info("SEC EDGAR submissions failed: %s", exc)

    return (_ok("financial_filings", items) if items
            else _empty("financial_filings", "No financial data found (likely private or non-US)"))


def get_linkedin_signals(company_name: str) -> dict:
    """
    Retrieve publicly indexed LinkedIn signals via site:linkedin.com search (Serper → Tavily).
    Args:
        company_name: Full company name e.g. 'Stripe Inc'
    """
    if not company_name:
        return _err("linkedin_signals", "No company name")

    query = f"site:linkedin.com {company_name}"

    if SERPER_API_KEY:
        try:
            r = _post(
                "https://google.serper.dev/search",
                json    = {"q": query, "num": 10},
                headers = {"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            )
            items = [
                _item(title=x.get("title",""), summary=x.get("snippet",""), url=x.get("link",""))
                for x in r.json().get("organic", [])
                if "linkedin.com" in x.get("link", "")
            ][:10]
            if items:
                return _ok("linkedin_signals", items)
        except Exception as exc:
            log.info("Serper LinkedIn failed: %s", exc)

    if TAVILY_API_KEY:
        try:
            r = _post(
                "https://api.tavily.com/search",
                json    = {"api_key": TAVILY_API_KEY, "query": query, "search_depth": "basic", "max_results": 10},
                headers = {"Content-Type": "application/json"},
            )
            items = [
                _item(title=x.get("title",""), summary=x.get("content",""), url=x.get("url",""))
                for x in r.json().get("results", [])
                if "linkedin.com" in x.get("url", "")
            ][:10]
            return _ok("linkedin_signals", items)
        except Exception as exc:
            return _err("linkedin_signals", str(exc))

    return _err("linkedin_signals", "SERPER_API_KEY and TAVILY_API_KEY both unset")


def get_company_registry(company_name: str, domain: str = "") -> dict:
    """
    Look up company registration data via Explorium.ai + Scrapfly.
    Primary:   Explorium.ai — firmographics + funding rounds.
    Secondary: Scrapfly (JS-rendered, residential proxy) — OpenCorporates,
               Companies House, SEC EDGAR.
    """
    if not company_name:
        return _err("company_registry", "No company name")

    items = []

    if EXPLORIUM_API_KEY:
        _EXPL_HEADERS = {
            "api_key":      EXPLORIUM_API_KEY,
            "Content-Type": "application/json",
            "accept":       "application/json",
        }
        _EXPL_BASE = "https://api.explorium.ai/v1"
        try:
            match_payload = {"businesses_to_match": [{"name": company_name}]}
            if domain:
                match_payload["businesses_to_match"][0]["website"] = domain  # fixed: was "domain"

            r_match = requests.post(
                f"{_EXPL_BASE}/businesses/match",
                headers=_EXPL_HEADERS, json=match_payload, timeout=15,
            )
            if r_match.status_code == 422:
                try:
                    detail = r_match.json()
                except Exception:
                    detail = r_match.text[:300]
                log.info("Explorium match 422 — schema mismatch. Response: %s", detail)
            else:
                r_match.raise_for_status()
                matched     = r_match.json().get("matched_businesses", [])
                business_id = matched[0].get("business_id") if matched else None

                if business_id:
                    log.info("Explorium matched '%s' → business_id=%s", company_name, business_id)
                    try:
                        r_firm = requests.post(
                            f"{_EXPL_BASE}/businesses/firmographics/enrich",
                            headers=_EXPL_HEADERS, json={"business_id": business_id}, timeout=15,
                        )
                        r_firm.raise_for_status()
                        firm = r_firm.json().get("data", {})
                        if firm:
                            items.append(_item(
                                title   = firm.get("name") or company_name,
                                summary = (
                                    f"Industry: {firm.get('linkedin_industry_category') or firm.get('naics_description', 'N/A')} | "
                                    f"Employees: {firm.get('number_of_employees_range', 'N/A')} | "
                                    f"Revenue: {firm.get('yearly_revenue_range', 'N/A')} | "
                                    f"Country: {firm.get('country_name', 'N/A')} | "
                                    f"Ticker: {firm.get('ticker', 'N/A')}"
                                ),
                                url  = firm.get("linkedin_profile") or firm.get("website") or f"https://{domain}",
                            ))
                    except Exception as exc:
                        log.info("Explorium firmographics failed: %s", exc)

                    try:
                        r_fund = requests.post(
                            f"{_EXPL_BASE}/businesses/funding-acquisitions/enrich",
                            headers=_EXPL_HEADERS, json={"business_id": business_id}, timeout=15,
                        )
                        if r_fund.status_code != 404:
                            r_fund.raise_for_status()
                            fund_data = r_fund.json().get("data", {})
                            rounds    = fund_data.get("funding_rounds") or []
                            if rounds:
                                latest = rounds[0]
                                items.append(_item(
                                    title   = f"Funding — {company_name}",
                                    summary = (
                                        f"Total raised: {fund_data.get('total_funding_amount', 'N/A')} | "
                                        f"Latest round: {latest.get('round_type', 'N/A')} "
                                        f"({latest.get('amount', 'N/A')}, {(latest.get('date') or '')[:10]}) | "
                                        f"Investors: {', '.join((latest.get('investors') or [])[:3]) or 'N/A'}"
                                    ),
                                    url  = f"https://{domain}" if domain else "",
                                    date = (latest.get("date") or "")[:10],
                                ))
                    except Exception as exc:
                        log.info("Explorium funding enrichment failed: %s", exc)
                else:
                    log.info("Explorium returned no business_id for '%s'", company_name)
        except Exception as exc:
            log.info("Explorium match failed: %s", exc)

    # ── Scrapfly — JS-rendered, residential proxy, TLD-aware targets ──────
    name_q = requests.utils.quote(company_name)
    tld = domain.rsplit(".", 1)[-1].lower() if domain else ""
    is_uk = tld == "uk" or domain.endswith(".co.uk")
    is_us_likely = tld in ("com", "us", "io", "ai", "net", "org")

    registry_targets = [
        ("OpenCorporates", f"https://opencorporates.com/companies?q={name_q}&action=go"),
    ]
    if is_uk:
        registry_targets.append((
            "Companies House",
            f"https://find-and-update.company-information.service.gov.uk/search?q={name_q}",
        ))
    if is_us_likely:
        registry_targets.append((
            "SEC EDGAR",
            f"https://www.sec.gov/cgi-bin/browse-edgar?company={name_q}&action=getcompany&type=10-K&dateb=&owner=include&count=5&search_text=",
        ))

    for label, url in registry_targets:
        text = _scrapfly(url, render_js=True, use_proxy_pool=True)
        if _is_useful(text):
            items.append(_item(
                title   = f"{label} — {company_name}",
                summary = _trunc(text, 300),
                url     = url,
            ))
            log.info("  ✓ company_registry %-18s — %d chars via Scrapfly", label, len(text))
        else:
            log.info("  ○ company_registry %-18s — no usable content for %s", label, url)

    return (
        _ok("company_registry", items) if items
        else _empty("company_registry", f"No registry data found for '{company_name}'")
    )


def get_tender_opportunities(company_name: str, keywords: str = "") -> dict:
    """
    Scrape live Indian government tenders from GeM Portal via Apify REST API.
    Call only if the company operates in or sells to Indian public sector.
    Args:
        company_name: Used to derive keywords when none provided
        keywords:     Optional category filter e.g. 'IT Software Cloud'
    """
    if not APIFY_API_TOKEN:
        return _err("tender_opportunities", "APIFY_API_TOKEN not set")

    search_terms = keywords or company_name
    if not search_terms:
        return _err("tender_opportunities", "No keywords or company name provided")

    try:
        run_url = f"https://api.apify.com/v2/acts/{_GEM_ACTOR}/run-sync-get-dataset-items"
        # Minimal input — only the fields we're confident the actor accepts.
        # Each extra field is a potential source of a 400 "input not valid"
        # rejection if the actor's schema doesn't declare it, so when in doubt
        # leave it out rather than guess.
        actor_input = {
            "maxTenders": 20,
            "category":   search_terms,
        }
        r = requests.post(
            run_url,
            params  = {"token": APIFY_API_TOKEN, "timeout": 300, "memory": 2048},
            json    = actor_input,
            timeout = 320,   # must exceed the Apify-side timeout above
            headers = {"Content-Type": "application/json"},
        )

        # A 404 here means the Apify actor itself (krawlify/gem-portal-scraper)
        # is missing, renamed, or unavailable on this account — that's a real
        # configuration problem, not "no tenders found", so it stays an error,
        # but with a clear human-readable message instead of the raw HTTP trace.
        if r.status_code == 404:
            return _err(
                "tender_opportunities",
                f"GeM Portal actor '{_GEM_ACTOR}' not found on Apify — check the actor ID/permissions"
            )

        # A 400 means the input payload failed Apify's schema validation for
        # this actor (unknown field, wrong type, etc). Apify's error body
        # normally names the offending field — surface that instead of a
        # bare "HTTP 400" so the real cause is visible in the UI/logs.
        if r.status_code == 400:
            try:
                detail = r.json()
                msg = (detail.get("error", {}).get("message")
                       or detail.get("message")
                       or str(detail)[:200])
            except Exception:
                msg = (r.text or "")[:200]
            return _err("tender_opportunities", f"Apify rejected the request input: {msg}")

        r.raise_for_status()
        dataset = r.json()

        if not isinstance(dataset, list):
            return _err("tender_opportunities", f"Unexpected response shape: {str(dataset)[:120]}")

        items = []
        for o in dataset[:15]:
            title = o.get("tenderTitle") or o.get("title") or o.get("name") or ""
            if not title:
                continue
            run_id   = o.get("_apify_run_id", "")
            item_url = (f"https://console.apify.com/actors/{_GEM_ACTOR}/runs/{run_id}"
                        if run_id else f"https://apify.com/store/{_GEM_ACTOR}")
            gem_url  = o.get("url") or o.get("gemUrl") or item_url

            items.append(_item(
                title   = title,
                summary = (f"Ministry: {o.get('ministry') or o.get('department','N/A')} | "
                           f"Category: {o.get('category','N/A')} | "
                           f"Deadline: {o.get('bidEndDate') or o.get('closingDate','N/A')} | "
                           f"EMD: {o.get('emdAmount') or o.get('estimatedValue','N/A')}"),
                url     = gem_url,
                date    = o.get("bidStartDate") or o.get("publishedDate", ""),
            ))

        # Apify ran successfully and returned a (possibly empty) list — a
        # genuinely empty dataset means no matching tenders exist right now,
        # which is informational, not an error.
        return (_ok("tender_opportunities", items) if items
                else _empty("tender_opportunities", "No matching tenders currently listed on GeM Portal"))

    except requests.exceptions.Timeout:
        return _err("tender_opportunities", "Apify actor timed out after 150s")
    except requests.exceptions.HTTPError as exc:
        # Any other non-404/400 HTTP failure (5xx, auth, etc.) is a genuine error.
        status = getattr(exc.response, "status_code", "unknown")
        return _err("tender_opportunities", f"Apify request failed (HTTP {status})")
    except Exception as exc:
        return _err("tender_opportunities", str(exc))


def get_industry_directory_data(company_name: str, domain: str = "") -> dict:
    """
    Scrape Crunchbase and G2 profiles via Scrapfly (JS-rendered, residential
    proxy — required to clear Crunchbase/G2's bot protection reliably).
    """
    if not company_name:
        return _err("industry_directory", "No company name")

    slug  = re.sub(r"[^a-z0-9\-]", "-", company_name.lower()).strip("-")
    items = []
    targets = [
        ("Crunchbase", f"https://www.crunchbase.com/organization/{slug}"),
        ("G2",         f"https://www.g2.com/products/{slug}/reviews"),
    ]

    for label, url in targets:
        text = _scrapfly(url, render_js=True, use_proxy_pool=True)
        if _is_useful(text, min_len=100):
            items.append(_item(
                title   = f"{label} — {company_name}",
                summary = _trunc(text, 300),
                url     = url,
            ))
            log.info("  ✓ industry_directory %-12s — %d chars via Scrapfly", label, len(text))
        else:
            log.info("  ○ industry_directory %-12s — no usable content for %s", label, url)

    return (
        _ok("industry_directory", items) if items
        else _empty("industry_directory", f"No directory profiles found for '{company_name}'")
    )


# ══════════════════════════════════════════════════════════════════════════════
#  AGENT
# ══════════════════════════════════════════════════════════════════════════════
try:
    from google.adk.agents   import Agent
    from google.adk.runners  import Runner
    from google.adk.sessions import InMemorySessionService
    from google.genai        import types as genai_types
    _ADK_OK = True
except ImportError:
    _ADK_OK = False


SYSTEM_PROMPT = """
You are a B2B account intelligence analyst with 8 tools. Call ALL of these
7 tools every time, in any order:
fetch_company_website(domain), search_company_news(company_name),
fetch_job_boards(company_name, domain), get_linkedin_signals(company_name),
get_company_registry(company_name, domain),
get_industry_directory_data(company_name, domain),
get_tender_opportunities(company_name, keywords)

Call fetch_financial_filings(company_name) for any US-based company that 
might be public or have financial records available. 

OUTPUT — return ONLY raw JSON, no markdown, no preamble:
{
  "company_summary":                "...",
  "current_initiatives":            "...",
  "expansion_signals":              "...",
  "hiring_signals":                 "...",
  "digital_transformation_signals": "...",
  "possible_pain_points":           "..."
}

RULES:
1. In "hiring_signals" only, tag each claim with the tool it came from in
   brackets, e.g. [job_boards]. No brackets in any other field.
2. Never fabricate — skip tools that returned "error" or "empty" when writing
   the brief.
3. job_boards items are either structured postings (weight roles ≤90 days
   old) or a single "Careers Page" text summary — treat both as valid
   hiring evidence; extract role/department mentions from the text form too.
4. Pain points: infer only, hedge ("may suggest", "could indicate").
"""


def _build_agent() -> "Agent":
    return Agent(
        name        = "account_research_agent",
        model       = "gemini-2.5-flash",
        instruction = SYSTEM_PROMPT,
        tools       = [
            fetch_company_website,
            search_company_news,
            fetch_job_boards,
            fetch_financial_filings,
            get_linkedin_signals,
            get_company_registry,
            get_tender_opportunities,
            get_industry_directory_data,
        ],
    )


async def _run_async(company_name: str, domain: str, country: str) -> dict:
    global _tool_statuses
    _tool_statuses = []

    agent       = _build_agent()
    session_svc = InMemorySessionService()
    runner      = Runner(agent=agent, app_name="ara", session_service=session_svc)
    session     = await session_svc.create_session(app_name="ara", user_id="ara_user")

    country_up = country.upper()
    context_notes = []
    if country_up in ("IN", "IND", "INDIA"):
        context_notes.append(
            "This company is based in India, which means Indian government "
            "tender/procurement signals may be relevant if it sells to the public sector."
        )
    elif country_up in ("US", "USA", "UNITED STATES"):
        context_notes.append(
            "This company is based in the US — if it is publicly listed, "
            "SEC filings may be available."
        )

    prompt = (
        f"Research this B2B account and produce the JSON brief.\n"
        f"Company: {company_name}\nDomain: {domain}\nCountry: {country}\n"
        + (f"Context: {' '.join(context_notes)}\n" if context_notes else "")
        + "Decide for yourself which tools are worth calling for this company "
        + "based on the context above, then call them.\n"
        + f"When you do call get_company_registry or get_industry_directory_data, "
        + f"pass domain='{domain}'."
    )

    msg = genai_types.Content(role="user", parts=[genai_types.Part(text=prompt)])

    log.info("─" * 50)
    log.info("Research started: %s (%s)", company_name, domain)
    log.info("─" * 50)
    t0 = time.time()

    final_text = ""
    async for event in runner.run_async(user_id="ara_user", session_id=session.id, new_message=msg):
        if event.is_final_response() and event.content and event.content.parts:
            final_text = event.content.parts[0].text

    elapsed = time.time() - t0
    log.info("─" * 50)
    log.info("Research complete in %.1fs", elapsed)
    log.info("─" * 50)

    # FIX: Use final_text instead of the undefined 'clean' variable
    # Also handles cleaning common JSON-breaking escapes from LLM output
    clean_json = final_text.replace("\\'", "'")
    try:
        return json.loads(clean_json)
    except json.JSONDecodeError:
        log.error("Agent returned non-JSON: %s…", final_text[:200])
        return {"raw_response": final_text, "error": "Agent did not return valid JSON"}


def run_research(company_name: str, domain: str, country: str = "US") -> dict:
    """Run the async ADK pipeline from a sync Flask context via a dedicated thread."""
    if not _ADK_OK:
        raise RuntimeError("google-adk not installed — run: pip install google-adk")

    result: list = [None, None]

    def _target():
        loop = asyncio.SelectorEventLoop() if sys.platform == "win32" else asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result[0] = loop.run_until_complete(_run_async(company_name, domain, country))
        except Exception as exc:
            result[1] = exc
        finally:
            pending = asyncio.all_tasks(loop)
            if pending:
                for t in pending:
                    t.cancel()
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join()

    if result[1]:
        raise result[1]
    return result[0]


# ══════════════════════════════════════════════════════════════════════════════
#  FLASK
# ══════════════════════════════════════════════════════════════════════════════
_SECTIONS = {
    "company_summary":                "## 1.1 Company Summary",
    "current_initiatives":            "## 1.2 Current Initiatives",
    "expansion_signals":              "## 1.3 Expansion Signals",
    "hiring_signals":                 "## 1.4 Hiring Signals",
    "digital_transformation_signals": "## 1.5 Digital Transformation Signals",
    "possible_pain_points":           "## 1.6 Possible Pain Points",
}

app = Flask(__name__, static_folder=".")
CORS(app)


@app.route("/")
def index():
    return send_from_directory(".", "index.html")


@app.route("/research", methods=["POST"])
def research():
    body        = request.get_json(force=True) or {}
    company     = (body.get("company") or body.get("company_name") or "").strip()
    raw_website = (body.get("website") or body.get("domain") or "").strip()

    if not company or not raw_website:
        return jsonify({"error": "company and website are required"}), 400

    domain  = re.sub(r"^https?://", "", raw_website).rstrip("/")
    website = raw_website if raw_website.startswith("http") else f"https://{raw_website}"
    # country = (body.get("country") or "US").strip()

    try:
        result = run_research(company, domain)

        if "raw_response" in result:
            brief = result["raw_response"]
        else:
            lines = [f"# Account Research Brief — {company}\n"]
            for key, heading in _SECTIONS.items():
                lines += [heading, result.get(key) or "_No public signal found._", ""]
            brief = "\n".join(lines)

        # ── Send tool_statuses INCLUDING items so frontend can render links ──
        # Items are kept here; large payloads are fine since they're < 300 chars each.
        tool_statuses = list(_tool_statuses)   # shallow copy, items included

        return jsonify({
            "company":        company,
            "website":        website,
            "brief":          brief,
            "tool_statuses":  tool_statuses,
            "generated_at":   datetime.now(timezone.utc).isoformat(),
        })
    except Exception as exc:
        traceback.print_exc()
        return jsonify({"error": str(exc)}), 500


@app.route("/health")
def health():
    return jsonify({"status": "ok", "adk_available": _ADK_OK})


if __name__ == "__main__":
    port = int(os.getenv("PORT", 5000))
    log.info("ARA running → http://localhost:%d", port)
    app.run(debug=True, port=port)