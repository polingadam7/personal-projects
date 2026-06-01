"""
Keyword Opportunity Finder
Identifies underserved niche keywords using the DataForSEO API.
Outputs a CSV ranked by composite opportunity score.
"""

import csv
import os
import time
import math
from dataclasses import dataclass, fields
from typing import Optional

import requests
from dotenv import load_dotenv

from seeds import SEED_KEYWORDS

load_dotenv()

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATAFORSEO_LOGIN = os.environ["DATAFORSEO_LOGIN"]
DATAFORSEO_PASSWORD = os.environ["DATAFORSEO_PASSWORD"]
BASE_URL = "https://api.dataforseo.com/v3"

SEARCH_VOLUME_ENDPOINT = f"{BASE_URL}/keywords_data/google_ads/search_volume/live"
KD_ENDPOINT = f"{BASE_URL}/dataforseo_labs/google/bulk_keyword_difficulty/live"

BATCH_SIZE = 1000          # DataForSEO max per request
KD_BATCH_SIZE = 1000
LOCATION_CODE = 2840       # United States
LANGUAGE_CODE = "en"

OUTPUT_FILE = "keyword_opportunities.csv"


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------
@dataclass
class KeywordResult:
    keyword: str
    monthly_volume: int = 0
    kd: int = 0
    cpc: float = 0.0
    competition: float = 0.0
    intent: str = ""
    opportunity_score: float = 0.0


# ---------------------------------------------------------------------------
# API client with exponential backoff
# ---------------------------------------------------------------------------
def _post(url: str, payload: list[dict], retries: int = 5) -> dict:
    auth = (DATAFORSEO_LOGIN, DATAFORSEO_PASSWORD)
    delay = 2.0
    for attempt in range(retries):
        try:
            resp = requests.post(url, json=payload, auth=auth, timeout=60)
            if resp.status_code == 429:
                wait = delay * (2 ** attempt)
                print(f"  Rate limited. Waiting {wait:.0f}s before retry {attempt + 1}/{retries}...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            if attempt == retries - 1:
                raise
            wait = delay * (2 ** attempt)
            print(f"  Request error ({exc}). Retrying in {wait:.0f}s...")
            time.sleep(wait)
    raise RuntimeError(f"Failed after {retries} retries: {url}")


# ---------------------------------------------------------------------------
# Search volume fetch
# ---------------------------------------------------------------------------
def fetch_search_volume(keywords: list[str]) -> dict[str, dict]:
    """Returns a dict keyed by keyword with volume/cpc/competition data."""
    results: dict[str, dict] = {}
    batches = _chunk(keywords, BATCH_SIZE)

    for i, batch in enumerate(batches, 1):
        print(f"Fetching search volume: batch {i}/{len(batches)} ({len(batch)} keywords)...")
        payload = [
            {
                "keywords": batch,
                "location_code": LOCATION_CODE,
                "language_code": LANGUAGE_CODE,
            }
        ]
        data = _post(SEARCH_VOLUME_ENDPOINT, payload)

        for task in data.get("tasks", []):
            if task.get("status_code") != 20000:
                print(f"  Task error: {task.get('status_message')}")
                continue
            for item in (task.get("result") or []):
                kw = item.get("keyword", "").lower().strip()
                if not kw:
                    continue
                monthly = item.get("search_volume") or 0
                cpc = item.get("cpc") or 0.0
                comp = item.get("competition") or 0.0
                # intent is not in search volume endpoint — will be blank
                results[kw] = {
                    "monthly_volume": int(monthly),
                    "cpc": float(cpc),
                    "competition": _parse_competition(comp),
                }

    return results


# ---------------------------------------------------------------------------
# Keyword difficulty fetch
# ---------------------------------------------------------------------------
def fetch_keyword_difficulty(keywords: list[str]) -> dict[str, int]:
    """Returns a dict keyed by keyword with KD score (0-100)."""
    results: dict[str, int] = {}
    batches = _chunk(keywords, KD_BATCH_SIZE)

    for i, batch in enumerate(batches, 1):
        print(f"Fetching keyword difficulty: batch {i}/{len(batches)} ({len(batch)} keywords)...")
        payload = [
            {
                "keywords": batch,
                "location_code": LOCATION_CODE,
                "language_code": LANGUAGE_CODE,
            }
        ]
        data = _post(KD_ENDPOINT, payload)

        for task in data.get("tasks", []):
            if task.get("status_code") != 20000:
                print(f"  Task error: {task.get('status_message')}")
                continue
            for item in (task.get("result") or []):
                kw = item.get("keyword", "").lower().strip()
                if not kw:
                    continue
                kd = item.get("keyword_difficulty") or 0
                results[kw] = int(kd)

    return results


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
def opportunity_score(volume: int, kd: int, cpc: float) -> float:
    """(volume * cpc) / (kd + 1) — higher is better."""
    if volume <= 0:
        return 0.0
    return round((volume * cpc) / (kd + 1), 4)


def infer_intent(cpc: float, competition: float) -> str:
    """Rough commercial intent label based on CPC and competition."""
    if cpc >= 2.0 and competition >= 0.5:
        return "commercial"
    if cpc >= 0.5 or competition >= 0.3:
        return "informational/commercial"
    return "informational"


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
_COMPETITION_MAP = {"LOW": 0.2, "MEDIUM": 0.5, "HIGH": 0.8}

def _parse_competition(value) -> float:
    if isinstance(value, str):
        return _COMPETITION_MAP.get(value.upper(), 0.0)
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _chunk(lst: list, size: int) -> list[list]:
    return [lst[i : i + size] for i in range(0, len(lst), size)]


def normalise_keywords(keywords: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for kw in keywords:
        normalised = kw.lower().strip()
        if normalised and normalised not in seen:
            seen.add(normalised)
            out.append(normalised)
    return out


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    keywords = normalise_keywords(SEED_KEYWORDS)
    print(f"Processing {len(keywords)} unique keywords...\n")

    # Fetch data
    volume_data = fetch_search_volume(keywords)
    print()
    kd_data = fetch_keyword_difficulty(keywords)
    print()

    # Merge into results
    results: list[KeywordResult] = []
    for kw in keywords:
        vd = volume_data.get(kw, {})
        volume = vd.get("monthly_volume", 0)
        cpc = vd.get("cpc", 0.0)
        competition = vd.get("competition", 0.0)
        kd = kd_data.get(kw, 0)
        intent = infer_intent(cpc, competition)
        score = opportunity_score(volume, kd, cpc)

        results.append(
            KeywordResult(
                keyword=kw,
                monthly_volume=volume,
                kd=kd,
                cpc=cpc,
                competition=round(competition, 4),
                intent=intent,
                opportunity_score=score,
            )
        )

    # Sort descending by opportunity score
    results.sort(key=lambda r: r.opportunity_score, reverse=True)

    # Write CSV
    fieldnames = [f.name for f in fields(KeywordResult)]
    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(
                {
                    "keyword": r.keyword,
                    "monthly_volume": r.monthly_volume,
                    "kd": r.kd,
                    "cpc": r.cpc,
                    "competition": r.competition,
                    "intent": r.intent,
                    "opportunity_score": r.opportunity_score,
                }
            )

    print(f"Done. Results saved to {OUTPUT_FILE}")
    print(f"\nTop 10 opportunities:")
    print(f"{'Rank':<5} {'Score':>10}  {'KW'}")
    print("-" * 70)
    for i, r in enumerate(results[:10], 1):
        print(f"{i:<5} {r.opportunity_score:>10.2f}  {r.keyword}")


if __name__ == "__main__":
    main()
