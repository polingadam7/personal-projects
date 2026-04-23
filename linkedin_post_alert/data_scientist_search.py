#!/usr/bin/env python3
"""
LinkedIn Post Alert - Data Scientist Hiring

Searches LinkedIn posts for hiring managers / recruiters actively looking for
remote data scientists in the US, filters with Claude, and emails a digest.

Usage:
    python data_scientist_search.py                         # full run + email
    python data_scientist_search.py --csv                   # save CSV, skip email
    python data_scientist_search.py --limit 10 --hours 48   # quick test
    python data_scientist_search.py --headless              # no visible browser
"""

import argparse
import csv
import json
import os
import random
import re
import smtplib
import time
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from urllib.parse import quote

import anthropic
from dotenv import load_dotenv
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

load_dotenv(Path(__file__).parent / ".env")

LINKEDIN_EMAIL    = os.getenv("LINKEDIN_EMAIL")
LINKEDIN_PASSWORD = os.getenv("LINKEDIN_PASSWORD")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
SMTP_HOST         = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT         = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER         = os.getenv("SMTP_USER")
SMTP_PASSWORD     = os.getenv("SMTP_PASSWORD")
ALERT_EMAIL_TO    = os.getenv("ALERT_EMAIL_TO")

SEARCH_QUERIES = [
    "hiring a data scientist",
    "looking for a data scientist",
    "growing our data team",
    "data scientist opening remote",
    "join our data science team",
    "seeking a data scientist",
    "data scientist role remote",
    "we are hiring data scientist",
]

MAX_SCROLLS_PER_QUERY = 40
SCROLL_PAUSE_S        = 2.5


def human_delay(min_s=1.0, max_s=3.0):
    time.sleep(random.uniform(min_s, max_s))


# LinkedIn post search supports only these datePosted buckets
_HOURS_TO_FILTER = [(24, "past-24h"), (168, "past-week"), (999, "past-month")]

def _hours_to_filter(hours):
    for threshold, value in _HOURS_TO_FILTER:
        if hours <= threshold:
            return value
    return "past-month"


def build_search_url(query, hours=24):
    date_filter = _hours_to_filter(hours)
    return (
        "https://www.linkedin.com/search/results/content/"
        f"?keywords={quote(query)}"
        f"&datePosted=%22{date_filter}%22"
        "&origin=FACETED_SEARCH"
        "&sortBy=%22date_posted%22"
    )

def login(page, email, password):
    print("Logging in to LinkedIn...")
    page.goto("https://www.linkedin.com/login", wait_until="domcontentloaded")
    human_delay(1, 2)
    page.fill("#username", email)
    human_delay(0.4, 0.9)
    page.fill("#password", password)
    human_delay(0.3, 0.7)
    page.click("button[type=submit]")
    try:
        page.wait_for_url("**/feed**", timeout=20000)
        print("Login successful.\n")
    except PlaywrightTimeoutError:
        print("Login did not redirect. Complete any CAPTCHA/2FA then press Enter.")
        input("Press Enter once you are on the LinkedIn feed... ")


# LinkedIn now uses data-view-name attributes instead of class-based selectors.
# The post card, commentary, and actor image elements are identified this way.
CARD_SELECTOR = '[role="listitem"][componentkey*="FeedType_FLAGSHIP_SEARCH"]'

_JS_EXTRACT = """(card) => {
    // Author profile URL - first <a> linking to a LI profile that contains a <figure>
    let profileUrl = '';
    const allLinks = card.querySelectorAll('a[href*="linkedin.com/in/"], a[href*="linkedin.com/company/"]');
    for (const a of allLinks) {
        if (a.querySelector('figure')) { profileUrl = a.href; break; }
    }

    // Author name and headline from the text link (no <figure> child)
    let authorName = '', authorHeadline = '';
    for (const link of allLinks) {
        if (link.querySelector('figure')) continue;  // skip the image link
        const paras = Array.from(link.querySelectorAll('p'))
            .map(p => p.textContent.trim())
            .filter(t => t.length > 0);
        if (paras.length > 0) authorName = paras[0];
        if (paras.length >= 2) {
            const candidates = paras.slice(1).filter(t => t.length > 20 && !/^\d+[smhdwmy]/.test(t));
            if (candidates.length > 0) authorHeadline = candidates[0];
        }
        break;
    }

    // Post text
    const commentary = card.querySelector('[data-testid="expandable-text-box"]');
    const postText = commentary ? commentary.innerText.trim() : '';

    // Post URL: direct feed/update link if available (reshares), else recent-activity
    const postLink = card.querySelector('a[href*="linkedin.com/feed/update/"]');
    let postUrl = postLink ? postLink.href : '';
    if (postUrl) { try { postUrl = postUrl.split('?')[0]; } catch(e) {} }
    if (!postUrl && profileUrl) {
        postUrl = profileUrl.replace(/\/$/, '') + '/recent-activity/all/';
    }

    const isCompany = profileUrl.includes('/company/');
    return { authorName, authorHeadline, postText, postUrl, profileUrl, isCompany };
}"""


def _extract_card_data(card):
    try:
        return card.evaluate(_JS_EXTRACT)
    except Exception:
        return {}


def _dump_debug(page, query, out_dir):
    """Save page HTML + a selector probe report for debugging."""
    safe = query.replace(" ", "_")[:30]
    html_path = out_dir / f"debug_{safe}.html"
    html_path.write_text(page.content(), encoding="utf-8")
    # Report which selectors find anything at all
    probes = [
        "li", "div[class*=result]", "div[class*=search]",
        "[data-view-name]", "[data-urn]", "[data-entity-urn]",
        "div[class*=reusable]", "div[class*=feed]", "div[class*=update]",
        "article", "section",
    ]
    print("  [debug] selector probe:")
    for sel in probes:
        try:
            n = len(page.query_selector_all(sel))
            if n:
                print(f"    {sel!r:45s} -> {n} elements")
        except Exception:
            pass
    print(f"  [debug] page HTML saved: {html_path}")


def scrape_query(page, query, hours=24, debug=False):
    print(f'  Searching: "{query}"')
    url = build_search_url(query, hours=hours)
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except PlaywrightTimeoutError:
        print("  Timeout — skipping this query.")
        return []

    # Wait for initial JS render
    human_delay(3, 4)

    # Scroll to load more posts; also click "Show more results" if it appears
    for scroll_num in range(MAX_SCROLLS_PER_QUERY):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(SCROLL_PAUSE_S)
        # Click any "Show more results" button LinkedIn injects
        for btn_text in ["Show more results", "Load more"]:
            try:
                btn = page.query_selector(f'button:has-text("{btn_text}")')
                if btn and btn.is_visible():
                    btn.click()
                    time.sleep(SCROLL_PAUSE_S)
                    break
            except Exception:
                pass
    human_delay(1, 2)

    try:
        found = page.query_selector_all(CARD_SELECTOR)
        cards = [c for c in found if c.is_visible()]
    except Exception:
        cards = []

    if not cards:
        if debug:
            _dump_debug(page, query, Path(__file__).parent)
        print(f"  -> 0 cards found (selector: {CARD_SELECTOR!r})")
        return []

    print(f"  [selector matched] {CARD_SELECTOR!r} -> {len(cards)} cards")
    posts, seen = [], set()
    for card in cards:
        data = _extract_card_data(card)
        if not data:
            continue
        # Skip company/org page accounts (job boards, aggregators, etc.)
        if data.get("isCompany"):
            continue
        post_text = data.get("postText", "")
        if not post_text or len(post_text) < 30:
            continue
        key = post_text[:120]
        if key in seen:
            continue
        seen.add(key)
        posts.append({
            "author_name":     data.get("authorName", ""),
            "author_headline": data.get("authorHeadline", ""),
            "post_text":       post_text,
            "post_url":        data.get("postUrl", ""),
            "query":           query,
        })
    print(f"  -> {len(posts)} posts collected")
    return posts

# ---------------------------------------------------------------------------
# Claude filtering
# ---------------------------------------------------------------------------

_JSON_EXAMPLE = '[{"index": 0, "relevant": true, "reason": "One sentence.", "author_role": "hiring manager"}]'

SYSTEM_PROMPT = (
    "You are a job-search assistant. Decide whether each LinkedIn post is from a "
    "hiring manager or recruiter actively looking to hire a remote data scientist "
    "(or closely related role: ML engineer, applied scientist, data science lead) "
    "for a US-based or remote position.\n\n"
    "Return ONLY a valid JSON array with one object per post:\n"
    + _JSON_EXAMPLE + "\n\n"
    "IS relevant: author has hiring authority AND there is a clear US/remote signal "
    "(says remote, mentions a US city/state, works at a US company, or the role is "
    "explicitly open to US remote candidates). Role must be data scientist or closely "
    "related.\n"
    "NOT relevant: job seeker posting their own resume, generic article, role clearly "
    "outside the US with no remote option (e.g. India, UK, EU onsite), non-English post, "
    "offshore staffing/recruitment agency, automated job board or aggregator account, "
    "unrelated role, spam. When location is ambiguous and non-US seems more likely, "
    "mark NOT relevant.\n"
    "author_role values: hiring manager | recruiter | other"
)


def filter_posts_with_claude(posts):
    if not posts:
        return []
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    relevant = []
    print(f"\nFiltering {len(posts)} unique posts with Claude...")
    batch_size = 8
    for batch_start in range(0, len(posts), batch_size):
        batch = posts[batch_start: batch_start + batch_size]
        lines = []
        for i, p in enumerate(batch):
            lines.extend([
                f"--- POST {i} ---",
                f"Author: {p.get('author_name', '')}",
                f"Headline: {p.get('author_headline', '')}",
                f"Text: {p.get('post_text', '')[:700]}",
                "",
            ])
        user_msg = "Evaluate each post and return a JSON array.\n\n" + "\n".join(lines)
        try:
            response = client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=2000,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_msg}],
            )
            raw = response.content[0].text.strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
            raw = re.sub(r"\s*```$",          "", raw, flags=re.MULTILINE)
            results = json.loads(raw)
            for item in results:
                idx = item.get("index", -1)
                if not isinstance(idx, int) or not (0 <= idx < len(batch)):
                    continue
                if item.get("relevant"):
                    post = batch[idx].copy()
                    post["claude_reason"] = item.get("reason", "")
                    post["author_role"]   = item.get("author_role", "")
                    relevant.append(post)
        except json.JSONDecodeError as e:
            print(f"  [warn] JSON parse error: {e} — skipping batch")
        except Exception as e:
            print(f"  [error] Claude API failed: {e}")
        human_delay(0.5, 1.0)
    print(f"  -> {len(relevant)} posts passed the filter")
    return relevant

# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def save_csv(posts, path):
    fields = ["author_name", "author_headline", "author_role", "claude_reason",
              "post_url", "query", "post_text"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(posts)
    print(f"CSV saved: {path}")


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

def _esc(text):
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\n", "<br>")
    )


def build_html_email(posts):
    today = datetime.now().strftime("%B %d, %Y")
    if not posts:
        return (
            "<html><body style='font-family:sans-serif;padding:20px'>"
            f"<h2 style='color:#0077b5'>LinkedIn DS Hiring Alert - {today}</h2>"
            "<p>No relevant posts found. Try again later!</p>"
            "</body></html>"
        )
    role_colors = {"hiring manager": "#0077b5", "recruiter": "#f5a623"}
    cards = []
    for p in posts:
        url     = p.get("post_url", "")
        role    = p.get("author_role", "")
        color   = role_colors.get(role, "#888")
        link    = f'<a href="{url}" style="color:#0077b5">View Post</a>' if url else ""
        badge   = (
            f'<span style="background:{color};color:white;padding:2px 8px;'
            f'border-radius:4px;font-size:11px;margin-left:8px">{_esc(role)}</span>'
        ) if role else ""
        text    = p.get("post_text", "")
        preview = _esc(text[:600]) + ("..." if len(text) > 600 else "")
        name    = _esc(p.get("author_name", "Unknown"))
        hl      = _esc(p.get("author_headline", ""))
        reason  = _esc(p.get("claude_reason", ""))
        cards.append(
            "<div style='border:1px solid #e0e0e0;border-radius:8px;padding:18px;"
            "margin-bottom:18px;background:#fafafa'>"
            f"<div style='margin-bottom:10px'><strong style='font-size:15px'>{name}</strong>"
            f"{badge}<br><span style='color:#555;font-size:13px'>{hl}</span></div>"
            f"<div style='font-size:14px;color:#333;line-height:1.5;margin-bottom:10px'>{preview}</div>"
            f"<div style='font-size:12px;color:#777;margin-bottom:8px'><em>Why relevant: {reason}</em></div>"
            f"<div style='font-size:13px'>{link}</div></div>"
        )
    return (
        "<html><body style='font-family:sans-serif;max-width:680px;margin:auto;padding:24px;color:#222'>"
        "<h2 style='color:#0077b5;margin-bottom:4px'>LinkedIn DS Hiring Alert</h2>"
        f"<p style='color:#888;margin-top:0'>{today} - {len(posts)} relevant post(s) found</p>"
        "<hr style='border:none;border-top:1px solid #eee;margin-bottom:20px'>"
        + "".join(cards)
        + "<p style='font-size:11px;color:#bbb;margin-top:24px'>Generated by data_scientist_search.py</p>"
        "</body></html>"
    )


def send_email(html):
    subject = f"LinkedIn DS Hiring Alert - {datetime.now().strftime('%b %d, %Y')}"
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_USER
    msg["To"]      = ALERT_EMAIL_TO
    msg.attach(MIMEText(html, "html"))
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, ALERT_EMAIL_TO, msg.as_string())
    print(f"Email sent to {ALERT_EMAIL_TO}")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(headless=False, dry_run=False, csv_out=False, limit=None, hours=24, debug=False):
    missing = [
        name for val, name in [
            (LINKEDIN_EMAIL,    "LINKEDIN_EMAIL"),
            (LINKEDIN_PASSWORD, "LINKEDIN_PASSWORD"),
            (ANTHROPIC_API_KEY, "ANTHROPIC_API_KEY"),
        ] if not val
    ]
    if not dry_run and not csv_out:
        missing += [
            name for val, name in [
                (SMTP_USER,      "SMTP_USER"),
                (SMTP_PASSWORD,  "SMTP_PASSWORD"),
                (ALERT_EMAIL_TO, "ALERT_EMAIL_TO"),
            ] if not val
        ]
    if missing:
        raise ValueError(f"Missing .env variables: {', '.join(missing)}")

    all_posts, seen_keys = [], set()

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
        login(page, LINKEDIN_EMAIL, LINKEDIN_PASSWORD)

        window_label = _hours_to_filter(hours)
        cap_label    = str(limit) if limit else "all"
        print(f"Running {len(SEARCH_QUERIES)} queries | window: {window_label} | collect up to: {cap_label}\n")

        for query in SEARCH_QUERIES:
            if limit and len(all_posts) >= limit:
                break
            posts = scrape_query(page, query, hours=hours, debug=debug)
            for post in posts:
                if limit and len(all_posts) >= limit:
                    break
                key = post["post_text"][:120]
                if key not in seen_keys:
                    seen_keys.add(key)
                    all_posts.append(post)
            human_delay(2, 4)
        browser.close()

    print(f"\nTotal unique posts collected: {len(all_posts)}")
    relevant = filter_posts_with_claude(all_posts)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    if csv_out or dry_run:
        csv_path = Path(__file__).parent / f"posts_{ts}.csv"
        save_csv(relevant, csv_path)

    if dry_run:
        html = build_html_email(relevant)
        preview_path = Path(__file__).parent / "preview.html"
        preview_path.write_text(html, encoding="utf-8")
        print(f"[dry-run] Email preview: {preview_path}")
    elif not csv_out:
        html = build_html_email(relevant)
        send_email(html)
        print(f"\nDone. {len(relevant)} post(s) emailed to {ALERT_EMAIL_TO}.")

    print(f"\nFinished — {len(relevant)} relevant post(s) out of {len(all_posts)} collected.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="LinkedIn post alert for data scientist hiring managers."
    )
    parser.add_argument("--headless", action="store_true",
                        help="Run without a visible browser.")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Save CSV + preview.html, skip email (no SMTP needed).")
    parser.add_argument("--csv",      action="store_true",
                        help="Save results to CSV instead of emailing (no SMTP needed).")
    parser.add_argument("--limit",    type=int, default=None, metavar="N",
                        help="Stop collecting after N total posts (e.g. --limit 10 for a quick test).")
    parser.add_argument("--hours",    type=int, default=168,  metavar="N",
                        help="Time window: <=24 -> past-24h, <=168 -> past-week, else past-month (default: 24).")
    parser.add_argument("--debug", action="store_true",
                        help="On 0-result queries: dump page HTML + selector probe to debug_*.html.")
    args = parser.parse_args()
    run(
        headless=args.headless,
        dry_run=args.dry_run,
        csv_out=args.csv,
        limit=args.limit,
        hours=args.hours,
        debug=args.debug,
    )
