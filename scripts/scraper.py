#!/usr/bin/env python3
"""
Work in Germany — Automated Job Scraper

Single script that:
1. Scrapes Greenhouse + Lever public APIs for 100+ companies
2. Filters for Germany-based positions
3. Classifies by category (SWE, DS, ML, Infra, etc.)
4. Detects language requirements and visa signals
5. Saves structured data to jobs.json
6. Generates the full README.md with job tables

Usage:
    python scripts/scraper.py                  # Full run
    python scripts/scraper.py --dry-run        # Preview only
    python scripts/scraper.py --board celonis  # Single company
"""

import argparse
import base64
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import requests
import yaml

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
ROOT = Path(__file__).parent.parent
CONFIG_PATH = ROOT / "config.yml"
JOBS_PATH = ROOT / "data" / "jobs.json"
STATS_PATH = ROOT / "data" / "stats.json"
README_PATH = ROOT / "README.md"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-5s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("scraper")

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "WorkInGermany-Bot/1.0 (+https://github.com/ashishtiwari03/work-in-germany)",
    "Accept": "application/json",
})


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------
def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Location detection — is this job in Germany?
# ---------------------------------------------------------------------------
def is_germany(location: str, config: dict) -> bool:
    """Check if a location string refers to Germany."""
    loc = location.lower()

    # Direct city match
    for city in config["germany_signals"]["cities"]:
        if city.lower() in loc:
            return True

    # Region match
    for region in config["germany_signals"]["regions"]:
        # Exact word boundary for short strings like "DE"
        if len(region) <= 3:
            if re.search(rf"\b{re.escape(region.lower())}\b", loc):
                return True
        elif region.lower() in loc:
            return True

    # Remote signals
    for signal in config["germany_signals"]["remote_signals"]:
        if signal.lower() in loc:
            return True

    return False


def normalize_location(location: str, config: dict) -> str:
    """Clean up location string for display."""
    loc = location
    for german, english in config.get("location_normalize", {}).items():
        loc = loc.replace(german, english)
    # Strip excessive detail
    loc = re.sub(r"\s*,\s*(Germany|Deutschland)\s*$", "", loc, flags=re.IGNORECASE)
    loc = loc.strip(" ,;")
    return loc if loc else "Germany"


# ---------------------------------------------------------------------------
# Category classification
# ---------------------------------------------------------------------------
def classify_category(title: str, config: dict) -> str:
    """Classify job title into a category."""
    t = title.lower()
    for cat_name, cat_cfg in config["categories"].items():
        for kw in cat_cfg.get("keywords", []):
            if kw.lower() in t:
                return cat_name
    return "Other"


# ---------------------------------------------------------------------------
# Language & visa detection from description
# ---------------------------------------------------------------------------
GERMAN_REQUIRED = [
    "deutsch erforderlich", "fließend deutsch", "verhandlungssicher deutsch",
    "german is required", "fluent german required", "german required",
    "german language required", "deutschkenntnisse auf c1", "deutsch auf c1",
    "deutsch auf muttersprachniveau",
]
GERMAN_PREFERRED = [
    "deutsch von vorteil", "german is a plus", "german preferred",
    "german is an advantage", "german nice to have", "deutschkenntnisse wünschenswert",
]
ENGLISH_OK = [
    "english-speaking", "english speaking environment", "working language is english",
    "english is our", "no german required", "international team",
    "english as the working language", "company language is english",
]
VISA_YES = [
    "visa sponsorship", "relocation support", "relocation package",
    "work permit assistance", "blue card", "we sponsor",
]
VISA_NO = [
    "eu work permit required", "must have existing right to work",
    "no visa sponsorship", "valid work permit required",
    "existing right to work in germany",
]


def detect_language(text: str) -> str:
    t = text.lower()
    for phrase in GERMAN_REQUIRED:
        if phrase in t:
            return "de_required"
    for phrase in GERMAN_PREFERRED:
        if phrase in t:
            return "de_preferred"
    for phrase in ENGLISH_OK:
        if phrase in t:
            return "en_ok"
    return "unknown"


def detect_visa(text: str, company_visa: bool) -> str:
    t = text.lower()
    for phrase in VISA_YES:
        if phrase in t:
            return "sponsor"
    for phrase in VISA_NO:
        if phrase in t:
            return "no_sponsor"
    return "sponsor" if company_visa else "unknown"


def strip_html(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"&[a-z]+;", " ", text)
    return re.sub(r"\s+", " ", text).strip()


# ---------------------------------------------------------------------------
# Greenhouse Scraper
# ---------------------------------------------------------------------------
def scrape_greenhouse(company: dict, config: dict) -> list[dict]:
    """Scrape a single Greenhouse board, return Germany jobs."""
    board = company["board"]
    name = company["name"]
    url = f"https://boards-api.greenhouse.io/v1/boards/{board}/jobs"

    try:
        resp = SESSION.get(url, params={"content": "true"}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"  [{name}] API error: {e}")
        return []

    raw_jobs = data.get("jobs", [])
    log.info(f"  [{name}] {len(raw_jobs)} total jobs on Greenhouse")

    results = []
    for raw in raw_jobs:
        loc_name = raw.get("location", {}).get("name", "")
        if not is_germany(loc_name, config):
            continue

        content = strip_html(raw.get("content", ""))
        posted = ""
        if raw.get("updated_at"):
            try:
                dt = datetime.fromisoformat(raw["updated_at"].replace("Z", "+00:00"))
                posted = dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        category = classify_category(raw.get("title", ""), config)
        language = detect_language(content)
        visa = detect_visa(content, company.get("visa", False))

        results.append({
            "id": f"gh_{board}_{raw.get('id', '')}",
            "company": name,
            "company_type": company.get("company_type", ""),
            "title": raw.get("title", ""),
            "location": normalize_location(loc_name, config),
            "url": raw.get("absolute_url", ""),
            "posted_at": posted,
            "category": category,
            "language": language,
            "visa": visa,
            "source": "greenhouse",
        })

    log.info(f"  [{name}] → {len(results)} Germany jobs")
    return results


# ---------------------------------------------------------------------------
# Lever Scraper
# ---------------------------------------------------------------------------
def scrape_lever(company: dict, config: dict) -> list[dict]:
    """Scrape a single Lever board, return Germany jobs."""
    company_id = company["company"]
    name = company["name"]
    url = f"https://api.lever.co/v0/postings/{company_id}"

    try:
        resp = SESSION.get(url, params={"mode": "json"}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.warning(f"  [{name}] API error: {e}")
        return []

    log.info(f"  [{name}] {len(data)} total jobs on Lever")

    results = []
    for raw in data:
        categories = raw.get("categories", {})
        loc_name = categories.get("location", "")
        if not is_germany(loc_name, config):
            continue

        desc = raw.get("descriptionPlain", "") or ""
        posted = ""
        created_at = raw.get("createdAt", 0)
        if created_at:
            try:
                dt = datetime.fromtimestamp(created_at / 1000, tz=timezone.utc)
                posted = dt.strftime("%Y-%m-%d")
            except (ValueError, OSError):
                pass

        category = classify_category(raw.get("text", ""), config)
        language = detect_language(desc)
        visa = detect_visa(desc, company.get("visa", False))

        results.append({
            "id": f"lv_{company_id}_{raw.get('id', '')}",
            "company": name,
            "company_type": company.get("company_type", ""),
            "title": raw.get("text", ""),
            "location": normalize_location(loc_name, config),
            "url": raw.get("hostedUrl", ""),
            "posted_at": posted,
            "category": category,
            "language": language,
            "visa": visa,
            "source": "lever",
        })

    log.info(f"  [{name}] → {len(results)} Germany jobs")
    return results


# ---------------------------------------------------------------------------
# Merge with existing data
# ---------------------------------------------------------------------------
def load_existing() -> dict:
    if JOBS_PATH.exists():
        with open(JOBS_PATH) as f:
            data = json.load(f)
        return {j["id"]: j for j in data.get("jobs", [])}
    return {}


def merge(existing: dict, new_jobs: list[dict], max_age: int = 60) -> list[dict]:
    now = datetime.now(timezone.utc)
    merged = dict(existing)

    for job in new_jobs:
        job["last_seen"] = now.isoformat()
        if job["id"] in merged:
            job["first_seen"] = merged[job["id"]].get("first_seen", job["last_seen"])
        else:
            job["first_seen"] = job["last_seen"]
        merged[job["id"]] = job

    # Prune stale
    cutoff = (now - timedelta(days=max_age)).timestamp()
    active = {}
    for jid, j in merged.items():
        try:
            ls = datetime.fromisoformat(j.get("last_seen", "")).timestamp()
            if ls >= cutoff:
                active[jid] = j
        except ValueError:
            active[jid] = j

    return sorted(active.values(), key=lambda j: j.get("posted_at") or "", reverse=True)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------
def compute_stats(jobs: list[dict]) -> dict:
    cats = {}
    for j in jobs:
        c = j.get("category", "Other")
        cats[c] = cats.get(c, 0) + 1

    return {
        "total_jobs": len(jobs),
        "visa_friendly": sum(1 for j in jobs if j.get("visa") == "sponsor"),
        "english_friendly": sum(1 for j in jobs if j.get("language") == "en_ok"),
        "companies_tracked": len(set(j["company"] for j in jobs)),
        "categories": cats,
        "last_updated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
    }


# ---------------------------------------------------------------------------
# README generation
# ---------------------------------------------------------------------------
COMPANY_ICONS = {"dax40": "🔥", "unicorn": "🚀", "international": "🌍", "mittelstand": "🏭", "startup": "💡"}
LANG_ICONS = {"en_ok": "🇬🇧", "de_required": "🇩🇪", "de_preferred": "🔄", "unknown": "—"}
VISA_ICONS = {"sponsor": "✅", "no_sponsor": "❌", "unknown": "❓"}


def format_date(d: str) -> str:
    if not d:
        return "—"
    try:
        posted = datetime.strptime(d, "%Y-%m-%d")
        delta = (datetime.now() - posted).days
        if delta == 0: return "Today"
        if delta == 1: return "1 day ago"
        if delta < 7: return f"{delta} days ago"
        return d
    except ValueError:
        return d


def job_row(j: dict) -> str:
    icon = COMPANY_ICONS.get(j.get("company_type", ""), "")
    company = f"{icon} {j['company']}".strip()
    lang = LANG_ICONS.get(j.get("language", "unknown"), "—")
    visa = VISA_ICONS.get(j.get("visa", "unknown"), "❓")
    posted = format_date(j.get("posted_at", ""))
    url = j.get("url", "")
    apply_link = f"[Apply]({url})" if url else "—"
    return f"| {company} | {j['title']} | {j['location']} | {lang} | {visa} | {posted} | {apply_link} |"


def generate_readme(jobs: list[dict], stats: dict, config: dict):
    """Generate the full README.md from scratch."""
    cats_order = [
        "Software Engineering",
        "Data Science & ML",
        "Data Engineering",
        "Infrastructure & SRE",
        "Product & Management",
        "Design & UX",
        "Business & Finance",
        "Marketing & Sales",
        "HR & Recruiting",
        "Research & Science",
        "Werkstudent & Internship",
        "Trainee & Ausbildung",
        "Hardware Engineering",
        "Other",
    ]

    total = stats["total_jobs"]
    visa_n = stats["visa_friendly"]
    eng_n = stats["english_friendly"]
    companies_n = stats["companies_tracked"]
    updated = stats["last_updated"]

    lines = []
    lines.append("# 🇩🇪 Work in Germany — Automated Job Board\n")
    lines.append(f"![Jobs](https://img.shields.io/badge/jobs-{total}-blue)")
    lines.append(f"![Visa Friendly](https://img.shields.io/badge/visa%20friendly-{visa_n}-green)")
    lines.append(f"![English OK](https://img.shields.io/badge/english%20OK-{eng_n}-orange)")
    lines.append(f"![Companies](https://img.shields.io/badge/companies-{companies_n}-purple)")
    lines.append(f"![Updated](https://img.shields.io/badge/updated-every%206%20hours-success)\n")
    lines.append(f"> **Fully automated** list of tech jobs in Germany, scraped from **{companies_n} company career pages** and updated every 6 hours.\n")
    lines.append("> 🔄 Unlike manual lists, this repo uses **100+ company APIs** (Greenhouse, Lever) and updates automatically via GitHub Actions.")
    lines.append("> Every listing is tagged with language requirements and visa sponsorship status.\n")
    lines.append("> 🙏 **Contribute** by submitting an [issue](../../issues/new/choose)! See [contribution guidelines](CONTRIBUTING.md).\n")
    lines.append("---\n")

    # Browse by category
    lines.append("## 📂 Browse Jobs by Category\n")
    for cat in cats_order:
        cat_icon = config["categories"].get(cat, {}).get("icon", "💼")
        count = stats["categories"].get(cat, 0)
        anchor = cat.lower().replace(" & ", "--").replace(" ", "-")
        lines.append(f"{cat_icon} [{cat}](#{anchor}) ({count})\n")
    lines.append("---\n")

    # Guides
    lines.append("## 📚 Guides & Resources\n")
    lines.append("| Guide | Description |")
    lines.append("|-------|-------------|")
    lines.append("| 📄 [CV & Anschreiben Guide](docs/CV_GUIDE.md) | How to write a German-format CV and cover letter |")
    lines.append("| 🛂 [Visa & Work Permit Guide](docs/VISA_GUIDE.md) | EU Blue Card, §18b, job seeker visa, Ausländerbehörde |")
    lines.append("| 💰 [Salary & Benefits Guide](docs/SALARY_GUIDE.md) | Brutto vs Netto, salary ranges by role & city |")
    lines.append("| 🏢 [German Work Culture](docs/CULTURE_GUIDE.md) | Probezeit, Kündigungsfrist, Arbeitszeugnis, unwritten rules |")
    lines.append("| 🏢 [Company Directory](docs/COMPANIES.md) | 50+ companies that hire internationals, with ratings |")
    lines.append("\n---\n")

    # Legend
    lines.append("## 🚀 Legend\n")
    lines.append("| Icon | Meaning |")
    lines.append("|------|---------|")
    lines.append("| 🔥 | DAX 40 / Major German Company |")
    lines.append("| 🚀 | Unicorn / High-Growth Startup |")
    lines.append("| 🌍 | International Company |")
    lines.append("| 🏭 | German Mittelstand |")
    lines.append("| 💡 | Startup |")
    lines.append("| 🇬🇧 | English OK |")
    lines.append("| 🇩🇪 | German Required |")
    lines.append("| 🔄 | German Preferred |")
    lines.append("| ✅ | Visa Sponsorship |")
    lines.append("| ❌ | No Visa Sponsorship |")
    lines.append("| ❓ | Unknown |")
    lines.append("\n---\n")

    # Job tables per category
    for cat in cats_order:
        cat_icon = config["categories"].get(cat, {}).get("icon", "💼")
        anchor = cat.lower().replace(" & ", "--").replace(" ", "-")
        cat_jobs = [j for j in jobs if j.get("category") == cat]

        lines.append(f"## {cat_icon} {cat}\n")
        lines.append("[Back to top](#-work-in-germany--automated-job-board)\n")
        lines.append("| Company | Role | Location | Lang | Visa | Posted | Apply |")
        lines.append("|---------|------|----------|------|------|--------|-------|")

        if cat_jobs:
            for j in cat_jobs:
                lines.append(job_row(j))
        else:
            lines.append("| *No jobs currently listed — check back soon!* | | | | | | |")

        lines.append("")

    # Footer
    lines.append("---\n")
    lines.append(f"*Last updated: {updated} • Total: {total} jobs across {companies_n} companies*\n")
    lines.append("*Built with ❤️ for everyone trying to build a career in Germany.*\n")

    with open(README_PATH, "w") as f:
        f.write("\n".join(lines))

    log.info(f"README.md generated with {total} jobs")


# ---------------------------------------------------------------------------
# Arbeitsagentur API (Germany's federal job board — MASSIVE volume)
# Free public API, no auth needed, just X-API-Key header
# Docs: https://github.com/bundesAPI/jobsuche-api
# ---------------------------------------------------------------------------
ARBEITSAGENTUR_API = "https://rest.arbeitsagentur.de/jobboerse/jobsuche-service/pc/v4/app/jobs"
ARBEITSAGENTUR_HEADERS = {
    "User-Agent": "Jobsuche/2.9.2 (de.arbeitsagentur.jobboerse; build:1077; iOS 15.1.0) Alamofire/5.4.4",
    "Host": "rest.arbeitsagentur.de",
    "X-API-Key": "jobboerse-jobsuche",
    "Connection": "keep-alive",
}


def scrape_arbeitsagentur(search_terms: list[dict], config: dict) -> list[dict]:
    """
    Scrape Arbeitsagentur for each search term (what=keyword, wo=city).
    This is Germany's official federal job board — hundreds of thousands of jobs.
    """
    results = []
    seen_refs = set()

    for term in search_terms:
        keyword = term["was"]
        city = term.get("wo", "")
        params = {
            "angebotsart": "1",
            "was": keyword,
            "wo": city,
            "umkreis": term.get("umkreis", "50"),
            "size": str(term.get("size", 100)),
            "page": "1",
            "pav": "false",
            "veroeffentlichtseit": str(term.get("days", 7)),
        }

        try:
            resp = requests.get(
                ARBEITSAGENTUR_API,
                headers=ARBEITSAGENTUR_HEADERS,
                params=params,
                timeout=30,
                verify=False,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.warning(f"  [Arbeitsagentur] '{keyword}' in '{city}': {e}")
            continue

        stellenangebote = data.get("stellenangebote", [])
        total = data.get("maxErgebnisse", 0)
        log.info(f"  [Arbeitsagentur] '{keyword}' in '{city}': {len(stellenangebote)} jobs (of {total} total)")

        for job in stellenangebote:
            refnr = job.get("refnr", "")
            if refnr in seen_refs:
                continue
            seen_refs.add(refnr)

            title = job.get("titel", "")
            company_name = job.get("arbeitgeber", "Unknown Employer")
            location = job.get("arbeitsort", {})
            city_name = location.get("ort", city)
            plz = location.get("plz", "")

            # Build apply URL
            encoded_ref = base64.b64encode(refnr.encode()).decode()
            apply_url = f"https://www.arbeitsagentur.de/jobsuche/jobdetail/{encoded_ref}"

            # Dates
            posted = job.get("eintrittsdatum", "") or job.get("modifikationsTimestamp", "")
            if posted and "T" in posted:
                posted = posted.split("T")[0]

            # External URL if available
            ext_url = job.get("externeUrl", "")
            final_url = ext_url if ext_url else apply_url

            category = classify_category(title, config)

            results.append({
                "id": f"ba_{refnr}",
                "company": company_name,
                "company_type": "",
                "title": title,
                "location": city_name,
                "url": final_url,
                "posted_at": posted,
                "category": category,
                "language": "unknown",
                "visa": "unknown",
                "source": "arbeitsagentur",
            })

        time.sleep(0.5)

    log.info(f"  [Arbeitsagentur] Total: {len(results)} unique jobs")
    return results


# ---------------------------------------------------------------------------
# Personio ATS (used by many German startups/Mittelstand)
# Public XML/JSON feed at: https://{company}.jobs.personio.de/xml
# ---------------------------------------------------------------------------
def scrape_personio(company: dict, config: dict) -> list[dict]:
    """Scrape a single Personio jobs feed."""
    slug = company["slug"]
    name = company["name"]
    url = f"https://{slug}.jobs.personio.de/xml"

    try:
        resp = SESSION.get(url, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        log.warning(f"  [{name}] Personio error: {e}")
        return []

    # Parse XML
    from xml.etree import ElementTree as ET
    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        log.warning(f"  [{name}] Personio XML parse error: {e}")
        return []

    results = []
    positions = root.findall(".//position")
    log.info(f"  [{name}] {len(positions)} total Personio jobs")

    for pos in positions:
        job_id = pos.findtext("id", "")
        title = pos.findtext("name", "")
        office = pos.findtext("office", "")
        department = pos.findtext("department", "")
        desc = pos.findtext("jobDescriptions/jobDescription/value", "") or ""
        apply_url = pos.findtext("url", "") or f"https://{slug}.jobs.personio.de/job/{job_id}"
        created = pos.findtext("createdAt", "")

        # All Personio jobs are Germany-based by default (it's a German ATS)
        # but still check
        if not is_germany(office, config) and office:
            continue

        location = normalize_location(office, config) if office else "Germany"
        posted = created.split("T")[0] if created and "T" in created else created

        results.append({
            "id": f"ps_{slug}_{job_id}",
            "company": name,
            "company_type": company.get("company_type", ""),
            "title": title,
            "location": location,
            "url": apply_url,
            "posted_at": posted,
            "category": classify_category(title, config),
            "language": detect_language(desc),
            "visa": detect_visa(desc, company.get("visa", False)),
            "source": "personio",
        })

    log.info(f"  [{name}] → {len(results)} Germany jobs")
    return results


# ---------------------------------------------------------------------------
# SmartRecruiters (used by many EU companies)
# Public API: https://api.smartrecruiters.com/v1/companies/{id}/postings
# ---------------------------------------------------------------------------
def scrape_smartrecruiters(company: dict, config: dict) -> list[dict]:
    """Scrape SmartRecruiters public postings API."""
    company_id = company["company_id"]
    name = company["name"]
    url = f"https://api.smartrecruiters.com/v1/companies/{company_id}/postings"

    results = []
    offset = 0
    limit = 100

    while True:
        try:
            resp = SESSION.get(url, params={"offset": offset, "limit": limit}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.warning(f"  [{name}] SmartRecruiters error: {e}")
            break

        postings = data.get("content", [])
        if not postings:
            break

        log.info(f"  [{name}] SmartRecruiters batch: {len(postings)} jobs (offset={offset})")

        for posting in postings:
            loc = posting.get("location", {})
            city = loc.get("city", "")
            country = loc.get("country", "")
            region = loc.get("region", "")
            loc_str = f"{city}, {region}, {country}".strip(", ")

            if country.upper() not in ("DE", "GERMANY", "DEUTSCHLAND") and not is_germany(loc_str, config):
                continue

            job_id = posting.get("id", "")
            title = posting.get("name", "")
            apply_url = posting.get("ref", "") or f"https://jobs.smartrecruiters.com/{company_id}/{job_id}"
            created = posting.get("releasedDate", "")
            posted = created.split("T")[0] if created and "T" in created else ""

            desc = posting.get("customField", [])
            desc_text = " ".join(str(d.get("value", "")) for d in desc) if desc else ""

            results.append({
                "id": f"sr_{company_id}_{job_id}",
                "company": name,
                "company_type": company.get("company_type", ""),
                "title": title,
                "location": normalize_location(city or "Germany", config),
                "url": apply_url,
                "posted_at": posted,
                "category": classify_category(title, config),
                "language": detect_language(desc_text),
                "visa": detect_visa(desc_text, company.get("visa", False)),
                "source": "smartrecruiters",
            })

        if len(postings) < limit:
            break
        offset += limit
        time.sleep(0.5)

    log.info(f"  [{name}] → {len(results)} Germany jobs")
    return results


# ---------------------------------------------------------------------------
# Workday (used by SAP, Siemens, BMW, Allianz, etc.)
# Each company has: https://{company}.wd{n}.myworkdayjobs.com/wday/cxs/{company}/{site}/jobs
# ---------------------------------------------------------------------------
def scrape_workday(company: dict, config: dict) -> list[dict]:
    """Scrape a Workday career site."""
    name = company["name"]
    base_url = company["base_url"]  # e.g. https://sap.wd3.myworkdayjobs.com
    site = company["site"]          # e.g. SAPCareers
    tenant = company["tenant"]      # e.g. sap

    search_url = f"{base_url}/wday/cxs/{tenant}/{site}/jobs"
    results = []
    offset = 0
    limit = 20

    while True:
        payload = {
            "appliedFacets": {
                "Location_Country": ["bc33aa3152ec42d4995f4791a106ed09"]  # Germany country ID (common)
            },
            "limit": limit,
            "offset": offset,
            "searchText": "",
        }
        # Some Workday instances use different country IDs — fallback to location filter
        # Try without facet first, filter by location text
        payload_simple = {"limit": limit, "offset": offset, "searchText": ""}

        try:
            resp = SESSION.post(search_url, json=payload_simple, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            log.warning(f"  [{name}] Workday error: {e}")
            break

        postings = data.get("jobPostings", [])
        total = data.get("total", 0)

        if not postings:
            break

        log.info(f"  [{name}] Workday batch: {len(postings)} jobs (offset={offset}, total={total})")

        for posting in postings:
            title = posting.get("title", "")
            loc_parts = posting.get("locationsText", "")
            external_path = posting.get("externalPath", "")
            posted_on = posting.get("postedOn", "")

            # Filter for Germany
            if not is_germany(loc_parts, config):
                continue

            apply_url = f"{base_url}/en-US{external_path}" if external_path else ""
            posted = posted_on.split("T")[0] if "T" in posted_on else posted_on

            results.append({
                "id": f"wd_{tenant}_{external_path.split('/')[-1] if external_path else offset}",
                "company": name,
                "company_type": company.get("company_type", "dax40"),
                "title": title,
                "location": normalize_location(loc_parts, config),
                "url": apply_url,
                "posted_at": posted,
                "category": classify_category(title, config),
                "language": "unknown",
                "visa": detect_visa("", company.get("visa", False)),
                "source": "workday",
            })

        if offset + limit >= total or len(postings) < limit:
            break
        offset += limit
        time.sleep(1.0)

    log.info(f"  [{name}] → {len(results)} Germany jobs")
    return results


# ---------------------------------------------------------------------------
# JobSpy — scrapes LinkedIn, Indeed, Glassdoor, StepStone via python-jobspy
# pip install python-jobspy
# ---------------------------------------------------------------------------
def scrape_jobspy(searches: list[dict], config: dict) -> list[dict]:
    """
    Use python-jobspy library to scrape LinkedIn, Indeed, Glassdoor, etc.
    Each search = {term, location, site_names, results_wanted}
    """
    try:
        from jobspy import scrape_jobs
    except ImportError:
        log.warning("[JobSpy] python-jobspy not installed. Run: pip install python-jobspy")
        return []

    results = []
    seen_urls = set()

    for search in searches:
        term = search.get("term", "software engineer")
        location = search.get("location", "Germany")
        sites = search.get("sites", ["linkedin", "indeed", "glassdoor"])
        count = search.get("results_wanted", 50)

        log.info(f"  [JobSpy] Searching '{term}' in '{location}' on {sites}")

        try:
            jobs_df = scrape_jobs(
                site_name=sites,
                search_term=term,
                location=location,
                results_wanted=count,
                country_indeed="Germany",
                hours_old=168,  # 7 days
            )
        except Exception as e:
            log.warning(f"  [JobSpy] Error scraping '{term}': {e}")
            continue

        if jobs_df is None or jobs_df.empty:
            log.info(f"  [JobSpy] No results for '{term}'")
            continue

        log.info(f"  [JobSpy] Got {len(jobs_df)} results for '{term}'")

        for _, row in jobs_df.iterrows():
            url = str(row.get("job_url", ""))
            if url in seen_urls or not url:
                continue
            seen_urls.add(url)

            title = str(row.get("title", ""))
            company_name = str(row.get("company", "Unknown"))
            loc = str(row.get("location", "Germany"))
            description = str(row.get("description", ""))
            date_posted = str(row.get("date_posted", ""))
            site = str(row.get("site", ""))

            # Normalize date
            posted = ""
            if date_posted and date_posted != "nan":
                try:
                    dt = datetime.strptime(date_posted[:10], "%Y-%m-%d")
                    posted = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

            results.append({
                "id": f"js_{site}_{hash(url) % 999999999}",
                "company": company_name,
                "company_type": "",
                "title": title,
                "location": normalize_location(loc, config),
                "url": url,
                "posted_at": posted,
                "category": classify_category(title, config),
                "language": detect_language(description),
                "visa": detect_visa(description, False),
                "source": f"jobspy_{site}",
            })

        time.sleep(2.0)

    log.info(f"  [JobSpy] Total: {len(results)} unique jobs")
    return results


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Scrape Germany tech jobs")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--board", help="Scrape single Greenhouse board")
    parser.add_argument("--source", help="Run only one source: greenhouse,lever,arbeitsagentur,personio,smartrecruiters,workday,jobspy")
    args = parser.parse_args()

    config = load_config()
    all_jobs: list[dict] = []
    run_all = not args.source and not args.board

    # --- 1. Greenhouse (100+ companies) ---
    if run_all or args.source == "greenhouse" or args.board:
        gh_companies = config.get("greenhouse", [])
        if args.board:
            gh_companies = [c for c in gh_companies if c["board"] == args.board]
        log.info(f"=== [1/7] Greenhouse: {len(gh_companies)} companies ===")
        for i, company in enumerate(gh_companies):
            jobs = scrape_greenhouse(company, config)
            all_jobs.extend(jobs)
            if i < len(gh_companies) - 1:
                time.sleep(1.0)

    # --- 2. Lever (7+ companies) ---
    if run_all or args.source == "lever":
        lv_companies = config.get("lever", [])
        log.info(f"=== [2/7] Lever: {len(lv_companies)} companies ===")
        for i, company in enumerate(lv_companies):
            jobs = scrape_lever(company, config)
            all_jobs.extend(jobs)
            if i < len(lv_companies) - 1:
                time.sleep(1.0)

    # --- 3. Arbeitsagentur (Germany's federal job board — HUGE) ---
    if run_all or args.source == "arbeitsagentur":
        ba_searches = config.get("arbeitsagentur", {}).get("searches", [])
        if ba_searches:
            log.info(f"=== [3/7] Arbeitsagentur: {len(ba_searches)} search terms ===")
            ba_jobs = scrape_arbeitsagentur(ba_searches, config)
            all_jobs.extend(ba_jobs)

    # --- 4. Personio (German startups/Mittelstand) ---
    if run_all or args.source == "personio":
        ps_companies = config.get("personio", [])
        if ps_companies:
            log.info(f"=== [4/7] Personio: {len(ps_companies)} companies ===")
            for i, company in enumerate(ps_companies):
                jobs = scrape_personio(company, config)
                all_jobs.extend(jobs)
                if i < len(ps_companies) - 1:
                    time.sleep(1.0)

    # --- 5. SmartRecruiters (EU companies) ---
    if run_all or args.source == "smartrecruiters":
        sr_companies = config.get("smartrecruiters", [])
        if sr_companies:
            log.info(f"=== [5/7] SmartRecruiters: {len(sr_companies)} companies ===")
            for i, company in enumerate(sr_companies):
                jobs = scrape_smartrecruiters(company, config)
                all_jobs.extend(jobs)
                if i < len(sr_companies) - 1:
                    time.sleep(1.0)

    # --- 6. Workday (DAX40 — SAP, Siemens, BMW, etc.) ---
    if run_all or args.source == "workday":
        wd_companies = config.get("workday", [])
        if wd_companies:
            log.info(f"=== [6/7] Workday: {len(wd_companies)} companies ===")
            for i, company in enumerate(wd_companies):
                jobs = scrape_workday(company, config)
                all_jobs.extend(jobs)
                if i < len(wd_companies) - 1:
                    time.sleep(1.5)

    # --- 7. JobSpy (LinkedIn, Indeed, Glassdoor, StepStone) ---
    if run_all or args.source == "jobspy":
        js_searches = config.get("jobspy", {}).get("searches", [])
        if js_searches:
            log.info(f"=== [7/7] JobSpy (LinkedIn/Indeed/Glassdoor): {len(js_searches)} searches ===")
            js_jobs = scrape_jobspy(js_searches, config)
            all_jobs.extend(js_jobs)

    log.info(f"\n{'='*60}")
    log.info(f"Total scraped: {len(all_jobs)} Germany jobs from all sources")
    log.info(f"{'='*60}")

    # Merge with existing
    existing = load_existing()
    max_age = config.get("settings", {}).get("max_age_days", 60)
    merged = merge(existing, all_jobs, max_age)

    stats = compute_stats(merged)
    log.info(f"Final: {stats['total_jobs']} jobs | {stats['companies_tracked']} companies | {stats['visa_friendly']} visa-friendly | {stats['english_friendly']} english-ok")

    if args.dry_run:
        log.info("DRY RUN — not saving")
        print(json.dumps(stats, indent=2))
        for j in merged[:10]:
            print(f"  [{j.get('source','?'):15s}] {j['company']:20s} | {j['title'][:50]:50s} | {j['location']}")
        return

    # Save
    JOBS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(JOBS_PATH, "w") as f:
        json.dump({"metadata": {"total": len(merged), "last_updated": stats["last_updated"]}, "jobs": merged}, f, indent=2, ensure_ascii=False)

    with open(STATS_PATH, "w") as f:
        json.dump(stats, f, indent=2)

    # Generate README
    generate_readme(merged, stats, config)

    log.info("✅ Done!")


if __name__ == "__main__":
    main()
