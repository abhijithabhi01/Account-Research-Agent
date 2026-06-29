import asyncio
import json
import logging
import os
import re
import sys
import threading
import time
import traceback
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

# ── GCP auth ──────────────────────────────────────────────────────────────────
_SA_FILE = Path(__file__).parent / "serviceaccount.json"
if _SA_FILE.exists():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(_SA_FILE)

os.environ.setdefault("GOOGLE_GENAI_USE_VERTEXAI", "true")
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", "us-central1")

# ── API keys ──────────────────────────────────────────────────────────────────
SERPER_API_KEY    = os.getenv("SERPER_API_KEY",    "")
TAVILY_API_KEY    = os.getenv("TAVILY_API_KEY",    "")
RAPIDAPI_KEY      = os.getenv("RAPIDAPI_KEY",      "")
FMP_API_KEY       = os.getenv("FMP_API_KEY",       "")
APIFY_API_TOKEN   = os.getenv("APIFY_API_TOKEN",   "")
EXPLORIUM_API_KEY = os.getenv("EXPLORIUM_API_KEY", "")   # explorium.ai enrichment
SEC_API_KEY       = os.getenv("SEC_API_KEY",       "")   # sec-api.io query & full-text

_GEM_ACTOR = "krawlify/gem-portal-scraper"
_UA        = {"User-Agent": "AccountResearchAgent/1.0"}

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
        "items":      items,
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


def _scrape(url: str) -> str:
    """Fetch a URL and return visible text (up to 1 200 chars)."""
    for scheme in ("https", "http"):
        target = re.sub(r"^https?://", f"{scheme}://", url)
        try:
            r    = requests.get(target, timeout=12, headers=_UA)
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


# ══════════════════════════════════════════════════════════════════════════════
#  TOOL FUNCTIONS  (8 — one per data source)
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
    Fetch open jobs from Greenhouse (primary) and JSearch/RapidAPI (fallback).
    Falls back to scraping the company careers page if both fail.
    Args:
        company_name: Company name e.g. 'Stripe'
        domain:       Company domain e.g. 'stripe.com' for careers page fallback
    """
    if not company_name:
        return _err("job_boards", "No company name provided")

    items = []
    slug  = re.sub(r"[^a-z0-9\-]", "", company_name.lower().replace(" ", "-"))

    try:
        r = _get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true")
        for job in r.json().get("jobs", [])[:12]:
            dept = (job.get("departments") or [{}])[0].get("name", "N/A")
            loc  = job.get("location", {}).get("name", "N/A")
            items.append(_item(
                title   = job.get("title", ""),
                summary = f"Dept: {dept} | Location: {loc}",
                url     = job.get("absolute_url", ""),
                date    = (job.get("updated_at", "") or "")[:10],
            ))
    except Exception as exc:
        log.info("Greenhouse failed for '%s': %s", slug, exc)

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

    # Fallback: scrape company careers page directly
    if not items and domain:
        text = _scrape(f"https://{domain}/careers")
        if text:
            items.append(_item(
                title   = f"{company_name} — Careers",
                summary = text,
                url     = f"https://{domain}/careers",
            ))

    return _ok("job_boards", items) if items else _err("job_boards", "No job data found")

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

    # ── 1. sec-api.io Query API (primary — richer metadata, direct filing links) ──
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

    # ── 2. FMP key metrics (adds financial ratios if ticker resolved) ──
    if FMP_API_KEY and len(items) < 6:
        try:
            r_s = _get("https://financialmodelingprep.com/api/v3/search",
                       params={"query": company_name, "limit": 3, "apikey": FMP_API_KEY})
            results = r_s.json()
            if results:
                ticker = results[0].get("symbol", "")
                r_m = _get(f"https://financialmodelingprep.com/api/v3/key-metrics/{ticker}",
                           params={"limit": 2, "apikey": FMP_API_KEY})
                for m in r_m.json()[:2]:
                    items.append(_item(
                        title   = f"FMP Key Metrics — {ticker} ({m.get('date','')})",
                        summary = (f"Revenue/share: {m.get('revenuePerShare','N/A')} | "
                                   f"PE: {m.get('peRatio','N/A')} | "
                                   f"MarketCap: {m.get('marketCap','N/A')} | "
                                   f"NetMargin: {m.get('netProfitMargin','N/A')}"),
                        url     = f"https://financialmodelingprep.com/financial-statements/{ticker}",
                        date    = m.get("date", ""),
                    ))
        except Exception as exc:
            log.info("FMP failed: %s", exc)

    # ── 3. Free SEC EDGAR submissions API (no key needed, last-resort fallback) ──
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
            else _err("financial_filings", "No financial data found (private or non-US)"))


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
    Look up company registration via OpenCorporates (free) and SEC EDGAR name search.
    Args:
        company_name: Legal company name e.g. 'Stripe Inc'
        domain:       Company domain — improves match accuracy
    """
    if not company_name:
        return _err("company_registry", "No company name")

    items = []

    # OpenCorporates — free, no key needed
    try:
        r    = _get(f"https://api.opencorporates.com/v0.4/companies/search?q={requests.utils.quote(company_name)}&per_page=5")
        data = r.json().get("results", {}).get("companies", [])
        for entry in data[:5]:
            co = entry.get("company", {})
            items.append(_item(
                title   = co.get("name", ""),
                summary = f"Jurisdiction: {co.get('jurisdiction_code','N/A')} | Type: {co.get('company_type','N/A')} | Status: {co.get('current_status','N/A')} | Incorporated: {co.get('incorporation_date','N/A')}",
                url     = co.get("opencorporates_url", ""),
                date    = co.get("incorporation_date", ""),
            ))
    except Exception as exc:
        log.info("OpenCorporates failed: %s", exc)

    # SEC EDGAR name search — free
    try:
        r   = _get("https://efts.sec.gov/LATEST/search-index?q=%22{}%22&dateRange=custom&startdt=2000-01-01&forms=10-K".format(requests.utils.quote(company_name)))
        hits = r.json().get("hits", {}).get("hits", [])
        for h in hits[:3]:
            src = h.get("_source", {})
            items.append(_item(
                title   = src.get("entity_name", company_name),
                summary = f"Form: {src.get('form_type','N/A')} | Filed: {src.get('file_date','N/A')} | CIK: {src.get('entity_id','N/A')}",
                url     = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company={requests.utils.quote(company_name)}&type=10-K",
                date    = src.get("file_date", ""),
            ))
    except Exception as exc:
        log.info("SEC name search failed: %s", exc)

    return _ok("company_registry", items) if items else _err("company_registry", f"No registry data found for '{company_name}'")


def get_tender_opportunities(company_name: str, keywords: str = "") -> dict:
    """
    Scrape live Indian government tenders from GeM Portal via Apify actor.
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
        from apify_client import ApifyClient
        client    = ApifyClient(APIFY_API_TOKEN)
        run       = client.actor(_GEM_ACTOR).call(run_input={
            "maxTenders": 20, 
            "category": search_terms,
            "ministry": "", 
            "minEmdAmount": None,
            "proxyConfig": {"useApifyProxy": True},
        })
        if not run:
            return _err("tender_opportunities", "Apify actor returned no result")

        items = []
        for o in client.dataset(run["defaultDatasetId"]).iterate_items():
            title = o.get("tenderTitle") or o.get("title") or o.get("name") or ""
            if not title:
                continue
            items.append(_item(
                title   = title,
                summary = f"Ministry: {o.get('ministry') or o.get('department','N/A')} | Category: {o.get('category','N/A')} | Deadline: {o.get('bidEndDate') or o.get('closingDate','N/A')} | EMD: {o.get('emdAmount') or o.get('estimatedValue','N/A')}",
                url     = o.get("url") or o.get("gemUrl", ""),
                date    = o.get("bidStartDate") or o.get("publishedDate", ""),
            ))
            if len(items) >= 15:
                break

        return _ok("tender_opportunities", items) if items else _err("tender_opportunities", "GeM Portal returned no results")
    except Exception as exc:
        return _err("tender_opportunities", str(exc))


def get_industry_directory_data(company_name: str, domain: str = "") -> dict:
    """
    Scrape Crunchbase and G2 profiles via direct requests for funding, category, and ratings.
    Args:
        company_name: Company name e.g. 'Stripe Inc'
        domain:       Company domain for accurate matching
    """
    if not company_name:
        return _err("industry_directory", "No company name")

    slug  = re.sub(r"[^a-z0-9\-]", "-", company_name.lower()).strip("-")
    items = []

    for label, url in [
        ("Crunchbase", f"https://www.crunchbase.com/organization/{slug}"),
        ("G2",         f"https://www.g2.com/products/{slug}/reviews"),
    ]:
        text = _scrape(url)
        if text and len(text) > 40:
            items.append(_item(title=f"{label} — {company_name}", summary=_trunc(text, 300), url=url))

    return _ok("industry_directory", items) if items else _err("industry_directory", f"No directory profiles found for '{company_name}'")


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
You are a B2B account intelligence analyst.

TOOLS — call in this order:
1. fetch_company_website       (always)
2. search_company_news         (always)
3. fetch_job_boards            (always; pass domain=)
4. get_linkedin_signals        (always)
5. get_company_registry        (always; pass domain=)
6. get_industry_directory_data (always; pass domain=)
7. fetch_financial_filings     (US public companies only)
8. get_tender_opportunities    (Indian public-sector companies only)

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
1. Tag every claim: [news] [company_website] [job_boards] [financial_filings]
   [linkedin_signals] [company_registry] [industry_directory] [tender_opportunities]
2. Never fabricate — skip tools that returned "error" or "empty".
3. Hiring: weight roles ≤ 90 days old; note seniority and department.
4. Pain points: infer only; hedge ("may suggest", "could indicate").
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
    _tool_statuses = []   # reset per request

    agent       = _build_agent()
    session_svc = InMemorySessionService()
    runner      = Runner(agent=agent, app_name="ara", session_service=session_svc)
    session     = await session_svc.create_session(app_name="ara", user_id="ara_user")

    country_up = country.upper()
    hints = []
    if country_up in ("IN", "IND", "INDIA"):
        hints.append("India-based — call get_tender_opportunities and weight those signals.")
    elif country_up in ("US", "USA", "UNITED STATES"):
        hints.append("US company — call fetch_financial_filings if publicly listed.")

    prompt = (
        f"Research this B2B account and produce the JSON brief.\n"
        f"Company: {company_name}\nDomain: {domain}\nCountry: {country}\n"
        + (f"Note: {' '.join(hints)}\n" if hints else "")
        + f"Pass domain='{domain}' to get_company_registry and get_industry_directory_data."
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

    clean = re.sub(r"```(?:json)?", "", final_text).strip().strip("`").strip()
    try:
        return json.loads(clean)
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
    country = (body.get("country") or "US").strip()

    try:
        result = run_research(company, domain, country)

        if "raw_response" in result:
            brief = result["raw_response"]
        else:
            lines = [f"# Account Research Brief — {company}\n"]
            for key, heading in _SECTIONS.items():
                lines += [heading, result.get(key) or "_No public signal found._", ""]
            brief = "\n".join(lines)

        # Strip raw items before sending to frontend (keep metadata only)
        tool_statuses = [
            {k: v for k, v in s.items() if k != "items"}
            for s in _tool_statuses
        ]

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