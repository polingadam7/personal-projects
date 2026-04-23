#!/usr/bin/env python3
"""
LinkedIn Data Scientist Job Scraper

Finds remote Data Scientist jobs in the US posted within the past week
with fewer than a configurable number of applicants.

Usage:
    python scraper.py
    python scraper.py --threshold 75
    python scraper.py --threshold 150 --max-jobs 500 --output my_jobs.csv
    python scraper.py --headless
"""

import argparse
import csv
import os
import random
import re
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

load_dotenv()

EMAIL = os.getenv("LINKEDIN_EMAIL")
PASSWORD = os.getenv("LINKEDIN_PASSWORD")

OUTPUT_DIR = Path(__file__).parent

# ---------------------------------------------------------------------------
# Agency / recruiter filter
# ---------------------------------------------------------------------------
# Companies whose name contains any of these strings are assumed to be
# staffing agencies / third-party recruiters and are skipped by default.
# Override with --include-agencies.
AGENCY_KEYWORDS = [
    "staffing",
    "recruiting",
    "recruitment",
    "robert half",
    "insight global",
    "kforce",
    "apex systems",
    "teksystems",
    "tek systems",
    "randstad",
    "adecco",
    "modis",
    "cybercoders",
    "manpower",
    "spherion",
    "allegis",
    "aerotek",
    "actalent",
    "alignerr",
    "remotehunter",
    "nexus consulting",
    "piper companies",
    "lensa",
    "fetchjobs",
    "jobgether",
    "wiraa",
]


def is_agency(company: str) -> bool:
    c = company.lower()
    return any(kw in c for kw in AGENCY_KEYWORDS)


# ---------------------------------------------------------------------------
# Contract position filter
# ---------------------------------------------------------------------------
# Detects contract / temp roles from the job title and location string.
# Override with --include-contract.
CONTRACT_KEYWORDS = [
    "contract",
    "contractor",
    "c2c",
    "corp to corp",
    "1099",
    "temporary",
    "temp role",
    "w2 contract",
]


def is_contract(title: str, location: str = "") -> bool:
    combined = (title + " " + location).lower()
    return any(kw in combined for kw in CONTRACT_KEYWORDS)


# ---------------------------------------------------------------------------
# URL builder
# ---------------------------------------------------------------------------

def build_search_url(start: int = 0, days: int = 3) -> str:
    seconds = days * 24 * 60 * 60
    return (
        "https://www.linkedin.com/jobs/search/"
        "?keywords=data%20scientist%20OR%20data%20analytics"
        "&location=United%20States"
        "&f_WT=2"               # Remote only
        "&f_JT=F"               # Full-time only (excludes contract/temp/part-time)
        f"&f_TPR=r{seconds}"    # Time window
        "&sortBy=R"             # Relevance sort
        f"&start={start}"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def human_delay(min_s: float = 1.0, max_s: float = 3.0):
    """Random sleep to mimic human pacing."""
    time.sleep(random.uniform(min_s, max_s))


def parse_applicant_count(text: str) -> int | None:
    """
    Parse applicant count from LinkedIn display text.

    Handles:
      "23 applicants"                    -> 23
      "Over 200 applicants"              -> 201  (intentionally over threshold)
      "Be among the first 25 applicants" -> 25
      "1,234 applicants"                 -> 1234
      "Actively reviewing applicants"    -> None  (unknown — kept by default)

    Returns None if count cannot be determined (job is still kept unless
    --skip-unknown is passed).
    """
    if not text:
        return None

    t = text.lower().strip()

    # LinkedIn status strings with no numeric count — return None (unknown)
    NO_COUNT_PHRASES = [
        "actively reviewing",
        "actively hiring",
        "hiring multiple candidates",
        "accepting applications",
        "access exclusive",      # LinkedIn Premium paywall upsell
    ]
    if any(phrase in t for phrase in NO_COUNT_PHRASES):
        return None

    # "over X applicants" — treat as X+1 so it gets filtered out correctly
    m = re.search(r'over\s+([\d,]+)\s+applicant', t)
    if m:
        return int(m.group(1).replace(',', '')) + 1

    # "be among the first X applicants"
    m = re.search(r'first\s+([\d,]+)\s+applicant', t)
    if m:
        return int(m.group(1).replace(',', ''))

    # plain "X applicants"
    m = re.search(r'([\d,]+)\s+applicant', t)
    if m:
        return int(m.group(1).replace(',', ''))

    # "over X people clicked apply / applied" — treat as X+1
    m = re.search(r'over\s+([\d,]+)\s+people\s+(?:clicked\s+apply|applied)', t)
    if m:
        return int(m.group(1).replace(',', '')) + 1

    # "X people clicked apply" / "X people applied"
    m = re.search(r'([\d,]+)\s+people\s+(?:clicked\s+apply|applied)', t)
    if m:
        return int(m.group(1).replace(',', ''))

    return None


# ---------------------------------------------------------------------------
# Login
# ---------------------------------------------------------------------------

def login(page, email: str, password: str):
    print("Logging in...")
    page.goto("https://www.linkedin.com/login", wait_until="domcontentloaded")
    human_delay(1, 2)

    page.fill("#username", email)
    human_delay(0.5, 1.2)
    page.fill("#password", password)
    human_delay(0.4, 0.9)
    page.click("button[type='submit']")

    try:
        page.wait_for_url("**/feed**", timeout=20000)
        print("Login successful.\n")
    except PlaywrightTimeoutError:
        print(
            "\n⚠  Login didn't auto-redirect. This may be a captcha or 2-FA prompt.\n"
            "   Complete it manually in the browser window, then press Enter here."
        )
        input("Press Enter once you're on the LinkedIn feed... ")


# ---------------------------------------------------------------------------
# Job detail extraction
# ---------------------------------------------------------------------------

# Ordered lists of selectors to try — LinkedIn changes class names regularly,
# so we fall through until one works.

TITLE_SELECTORS = [
    "h1.job-details-jobs-unified-top-card__job-title",
    "h1.t-24.t-bold",
    ".jobs-unified-top-card__job-title h1",
    "h1",
]

COMPANY_SELECTORS = [
    ".job-details-jobs-unified-top-card__company-name a",
    ".jobs-unified-top-card__company-name a",
    ".job-details-jobs-unified-top-card__company-name",
    ".jobs-unified-top-card__company-name",
]

LOCATION_SELECTORS = [
    ".job-details-jobs-unified-top-card__bullet",
    ".jobs-unified-top-card__bullet",
    ".job-details-jobs-unified-top-card__workplace-type",
]

POSTED_SELECTORS = [
    ".jobs-unified-top-card__posted-date",
    ".job-details-jobs-unified-top-card__primary-description-container span",
    "span.tvm__text--positive",
]

APPLICANT_SELECTORS = [
    ".jobs-unified-top-card__applicant-count",
    ".job-details-jobs-unified-top-card__applicant-count",
    ".jobs-unified-top-card__bullet--separator + span",
    ".tvm__text",
]

# LinkedIn changes these class names regularly — try each in order
CARD_SELECTORS = [
    "li.jobs-search-results__list-item",
    "li[data-occludable-job-id]",
    "div[data-job-id]",
    "li.ember-view.occludable-update",
    ".job-card-container",
    "ul.jobs-search-results__list > li",
    "li.scaffold-layout__list-item",
]


def _first_text(page, selectors: list[str]) -> str:
    """Return inner text of the first matching selector, or 'N/A'."""
    for sel in selectors:
        try:
            el = page.query_selector(sel)
            if el:
                t = el.inner_text().strip()
                if t:
                    return t
        except Exception:
            continue
    return "N/A"


def _has_number(text: str) -> bool:
    return bool(re.search(r'\d', text))


def _extract_count_from_text(text: str) -> str | None:
    """
    Pull a count string out of any LinkedIn applicant/apply text.
    Handles:
      "47 applicants"
      "Over 100 applicants"
      "Be among the first 25 applicants"
      "42 people clicked apply"
      "42 people applied"
    Returns None if no count found.
    """
    patterns = [
        r'((?:over\s+|first\s+)?[\d,]+\s+applicants?)',        # "47 applicants" / "over 100 applicants"
        r'((?:over\s+)?[\d,]+\s+people\s+clicked\s+apply)',    # "42 people clicked apply" / "over 100 people clicked apply"
        r'((?:over\s+)?[\d,]+\s+people\s+applied)',            # "42 people applied" / "over 100 people applied"
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def _is_count_line(line: str) -> bool:
    """Return True if a line looks like it contains an applicant/apply count."""
    lower = line.lower()
    return any(kw in lower for kw in [
        "applicant",
        "clicked apply",
        "people applied",
    ])


def card_applicant_hint(card) -> str:
    """
    Read the applicant count directly from the job card element *before* clicking.
    LinkedIn often shows the count on the card even when the detail panel shows
    only a status string like 'Actively reviewing applicants'.
    """
    try:
        text = card.inner_text()
        extracted = _extract_count_from_text(text)
        if extracted:
            return extracted
    except Exception:
        pass
    return ""


# Selectors for the right-side job detail panel — scoped so we don't
# accidentally read counts from other cards in the left panel list.
DETAIL_PANEL_SELECTORS = [
    ".jobs-details",
    ".job-view-layout",
    ".scaffold-layout__detail",
    ".jobs-search__right-rail",
    ".jobs-unified-top-card",
    "#job-details",
]


def _detail_panel_text(page) -> str:
    """Return inner text of the job detail panel (right side only)."""
    for sel in DETAIL_PANEL_SELECTORS:
        try:
            el = page.query_selector(sel)
            if el:
                t = el.inner_text()
                if t.strip():
                    return t
        except Exception:
            continue
    # Fall back to full body if no panel found
    return page.inner_text("body")


def _find_applicant_text(page, card_hint: str = "") -> str:
    """
    Search for applicant count text scoped to the job detail panel,
    strongly preferring lines with an actual number over status strings.

    card_hint: count string grabbed from the card before clicking (highest priority).
    """
    # card_hint is the most reliable source — use it immediately if numeric
    if card_hint and _has_number(card_hint):
        extracted = _extract_count_from_text(card_hint)
        if extracted:
            return extracted

    candidates: list[str] = []

    # Scope scan to detail panel only — avoids noise from left-panel cards
    try:
        panel_text = _detail_panel_text(page)
        for line in panel_text.splitlines():
            line = line.strip()
            if _is_count_line(line) and len(line) < 120:
                candidates.append(line)
    except Exception:
        pass

    # Also try known selectors directly
    for sel in APPLICANT_SELECTORS:
        try:
            el = page.query_selector(sel)
            if el:
                t = el.inner_text().strip()
                if _is_count_line(t) and len(t) < 120:
                    candidates.append(t)
        except Exception:
            continue

    if not candidates:
        return card_hint if card_hint else "N/A"

    # Join all candidate lines and do a single regex pass for a numeric count.
    # This is order-independent — finds any "X applicants" regardless of what
    # else is on the page (e.g. "Actively reviewing applicants" on the same panel).
    all_candidate_text = "\n".join(candidates)

    # Normalize LinkedIn Premium paywall upsell to a short label
    PAYWALL_PHRASES = ["access exclusive", "highest chance of hearing back"]
    if any(p in all_candidate_text.lower() for p in PAYWALL_PHRASES):
        return "hidden (LinkedIn Premium)"

    # Look for an explicit numeric count anywhere in the candidate text
    numeric = re.search(
        r'((?:over\s+)?(?:first\s+)?[\d,]+\s+(?:applicants?|people\s+clicked\s+apply|people\s+applied))',
        all_candidate_text,
        re.IGNORECASE,
    )
    if numeric:
        return numeric.group(1).strip()

    # No numeric count found — fall back to card hint if available
    if card_hint:
        return card_hint

    # Last resort: first status string from the detail panel
    return candidates[0]


def extract_job_details(page, card_hint: str = "", save_debug_html: bool = False) -> dict | None:
    """Extract structured data from the currently-open job detail panel."""
    try:
        page.wait_for_selector(
            "h1, .jobs-unified-top-card__job-title, .job-details-jobs-unified-top-card__job-title",
            timeout=6000,
        )
    except PlaywrightTimeoutError:
        return None

    # Give async content (applicant count, insights) time to render
    human_delay(1.0, 2.0)

    if save_debug_html:
        debug_path = OUTPUT_DIR / "debug_job_detail.html"
        debug_path.write_text(page.content(), encoding="utf-8")
        print(f"  [debug] Job detail HTML saved to: {debug_path}")

    applicant_text = _find_applicant_text(page, card_hint=card_hint)

    return {
        "title":           _first_text(page, TITLE_SELECTORS),
        "company":         _first_text(page, COMPANY_SELECTORS),
        "location":        _first_text(page, LOCATION_SELECTORS),
        "posted":          _first_text(page, POSTED_SELECTORS),
        "applicant_text":  applicant_text,
        "applicant_count": parse_applicant_count(applicant_text),
        "url":             page.url,
    }


# ---------------------------------------------------------------------------
# Main scrape loop
# ---------------------------------------------------------------------------

def scrape(
    threshold: int = 99,
    max_jobs: int = 300,
    max_results: int | None = None,
    days: int = 3,
    output_file: str = "jobs.csv",
    headless: bool = False,
    skip_unknown: bool = False,
    include_agencies: bool = False,
    include_contract: bool = False,
    debug: bool = False,
):
    """
    Scrape LinkedIn for remote Data Scientist jobs posted in the past week
    with fewer than `threshold` applicants.

    Args:
        threshold:        Max applicant count to keep a job (default 100).
        max_jobs:         Max number of job listings to inspect total (hard cap, default 300).
        max_results:      Stop early once this many jobs have been KEPT (optional).
        output_file:      Base name for the output CSV (timestamp appended).
        headless:         Run browser headlessly (default False — visible is safer).
        include_agencies: Keep staffing/recruiting agency postings (default: skip them).
        include_contract: Keep contract/temp positions (default: skip them).
    """
    if not EMAIL or not PASSWORD:
        raise ValueError(
            "LINKEDIN_EMAIL and LINKEDIN_PASSWORD must be set in your .env file.\n"
            "Copy .env.example to .env and fill in your credentials."
        )

    results = []
    inspected = 0
    start_offset = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()

        login(page, EMAIL, PASSWORD)

        print(
            f"Scanning remote Data Scientist jobs — past {days} days — "
            f"threshold: < {threshold} applicants\n"
            f"{'-' * 70}"
        )

        while inspected < max_jobs:
            url = build_search_url(start_offset, days=days)
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
            except PlaywrightTimeoutError:
                print("Page load timed out — retrying...")
                human_delay(3, 6)
                continue

            # Wait for at least one card selector to appear before proceeding
            found_selector = None
            for sel in CARD_SELECTORS:
                try:
                    page.wait_for_selector(sel, timeout=6000)
                    found_selector = sel
                    break
                except PlaywrightTimeoutError:
                    continue

            human_delay(2, 4)

            if not found_selector:
                print("Could not find job cards with any known selector.")
                if debug:
                    debug_path = OUTPUT_DIR / "debug_page.html"
                    debug_path.write_text(page.content(), encoding="utf-8")
                    print(f"Page HTML dumped to: {debug_path}")
                else:
                    print("Tip: run with --debug to dump the page HTML for inspection.")
                break

            job_cards = page.query_selector_all(found_selector)
            # Deduplicate: some selectors can match nested elements
            job_cards = [c for c in job_cards if c.is_visible()]

            if not job_cards:
                print("No more job cards found — search exhausted.")
                break

            print(f"\n[Page offset {start_offset}] {len(job_cards)} cards loaded (selector: {found_selector})...")

            for card in job_cards:
                if inspected >= max_jobs:
                    break

                try:
                    card.scroll_into_view_if_needed()
                    hint = card_applicant_hint(card)  # grab count before click
                    card.click()
                    human_delay(1.5, 3.0)

                    save_html = debug and inspected == 0
                    job = extract_job_details(page, card_hint=hint, save_debug_html=save_html)
                    if not job:
                        continue

                    inspected += 1
                    title = job["title"]
                    count = job["applicant_count"]
                    applicant_text = job["applicant_text"]

                    # Agency filter
                    if not include_agencies and is_agency(job["company"]):
                        print(f"  [agcy] {title[:45]:<45} @ {job['company'][:30]:<30} | skipped (agency)")
                        continue

                    # Contract filter
                    if not include_contract and is_contract(title, job.get("location", "")):
                        print(f"  [ctrn] {title[:45]:<45} @ {job['company'][:30]:<30} | skipped (contract)")
                        continue

                    # Decide keep/skip
                    # Explicit "Over X" guard — always skip regardless of parse result
                    _over = re.search(
                        r'\bover\s+[\d,]+\s+(?:applicants?|people)',
                        applicant_text,
                        re.IGNORECASE,
                    )
                    if _over:
                        tag = "skip"
                    elif count is not None and count >= threshold:
                        tag = "skip"
                    elif count is None and skip_unknown:
                        tag = "skip"
                    else:
                        tag = "KEPT"

                    if tag == "KEPT":
                        results.append(job)

                    count_display = applicant_text if applicant_text != "N/A" else "count unknown"
                    print(f"  [{tag}] {title[:45]:<45} @ {job['company'][:30]:<30} | {count_display}")

                    if max_results and len(results) >= max_results:
                        print(f"\nReached --max-results limit of {max_results}. Stopping early.")
                        break

                except Exception as e:
                    print(f"  [err ] Error processing card: {e}")
                    continue

            if max_results and len(results) >= max_results:
                break

            start_offset += 25
            human_delay(2, 5)

        browser.close()

    # ------------------------------------------------------------------
    # Write CSV
    # ------------------------------------------------------------------
    if not results:
        print("\nNo jobs matched the criteria.")
        return []

    fieldnames = ["title", "company", "location", "posted", "applicant_count", "applicant_text", "url"]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = Path(output_file).stem
    out_path = OUTPUT_DIR / f"{stem}_{ts}.csv"

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(results)

    print(
        f"\n{'=' * 70}\n"
        f"Done.  Inspected {inspected} jobs — {len(results)} kept (< {threshold} applicants).\n"
        f"Saved to: {out_path}\n"
    )
    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape LinkedIn for low-competition Data Scientist jobs."
    )
    parser.add_argument(
        "--threshold",
        type=int,
        default=99,
        help="Max applicant count to keep a listing (default: 99)",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=300,
        help="Max number of job listings to inspect (default: 300)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="jobs.csv",
        help="Output CSV filename base (default: jobs.csv)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser in headless mode (not recommended — harder for LinkedIn to pass)",
    )
    parser.add_argument(
        "--max-results",
        type=int,
        default=None,
        help="Stop once this many jobs have been KEPT (optional). E.g. --max-results 25",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=3,
        help="How many days back to search (default: 3)",
    )
    parser.add_argument(
        "--skip-unknown",
        action="store_true",
        help='Skip jobs where LinkedIn shows "Actively reviewing" instead of a count (default: keep them)',
    )
    parser.add_argument(
        "--include-agencies",
        action="store_true",
        help="Include staffing/recruiting agency postings (default: skip them)",
    )
    parser.add_argument(
        "--include-contract",
        action="store_true",
        help="Include contract/temp positions (default: skip them)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Dump page HTML to debug files when selectors fail or on first job detail",
    )
    args = parser.parse_args()

    scrape(
        threshold=args.threshold,
        max_jobs=args.max_jobs,
        max_results=args.max_results,
        days=args.days,
        output_file=args.output,
        headless=args.headless,
        skip_unknown=args.skip_unknown,
        include_agencies=args.include_agencies,
        include_contract=args.include_contract,
        debug=args.debug,
    )
