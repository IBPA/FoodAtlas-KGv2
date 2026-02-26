"""
Fetch publication date range for all PMIDs in the KG metadata files.

Uses NCBI E-utilities (esummary) to batch-query publication dates,
then reports the earliest and latest publication dates.

Usage:
    python analysis/get_pubdate_range.py
"""

import ast
import json
import time
from pathlib import Path

import pandas as pd
import requests

KG_DIR = Path(__file__).resolve().parent.parent / "outputs" / "kg"
BATCH_SIZE = 400  # NCBI recommends <=500 IDs per request
DELAY = 0.35  # seconds between requests (NCBI rate limit: 3 req/s without API key)
ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
CACHE_PATH = Path(__file__).resolve().parent / "pubdate_cache.json"


def extract_pmids() -> set[int]:
    """Extract all unique PMIDs from metadata TSV files."""
    pmids = set()
    for fname in ["metadata_diseases.tsv", "metadata_contains.tsv"]:
        fpath = KG_DIR / fname
        if not fpath.exists():
            print(f"  Skipping {fname} (not found)")
            continue
        df = pd.read_csv(fpath, sep="\t", usecols=["reference"])
        for ref in df["reference"].dropna():
            try:
                d = ast.literal_eval(ref)
                if isinstance(d, dict) and "pmid" in d:
                    pmids.add(int(d["pmid"]))
            except (ValueError, SyntaxError):
                pass
        print(f"  {fname}: running total {len(pmids)} unique PMIDs")
    return pmids


def load_cache() -> dict[str, str]:
    """Load previously fetched pubdates from cache."""
    if CACHE_PATH.exists():
        with open(CACHE_PATH) as f:
            return json.load(f)
    return {}


def save_cache(cache: dict[str, str]):
    with open(CACHE_PATH, "w") as f:
        json.dump(cache, f)


def fetch_pubdates(pmids: list[int], cache: dict[str, str]) -> dict[str, str]:
    """Fetch publication dates from NCBI esummary in batches."""
    # Filter out already-cached PMIDs
    to_fetch = [p for p in pmids if str(p) not in cache]
    print(f"\n  {len(pmids)} total PMIDs, {len(cache)} cached, {len(to_fetch)} to fetch")

    for i in range(0, len(to_fetch), BATCH_SIZE):
        batch = to_fetch[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(to_fetch) + BATCH_SIZE - 1) // BATCH_SIZE

        data_payload = {
            "db": "pubmed",
            "id": ",".join(str(p) for p in batch),
            "retmode": "json",
        }
        try:
            resp = requests.post(ESUMMARY_URL, data=data_payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, json.JSONDecodeError) as e:
            print(f"  Batch {batch_num}/{total_batches}: ERROR - {e}")
            time.sleep(1)
            continue

        result = data.get("result", {})
        found = 0
        for pmid_str, info in result.items():
            if pmid_str == "uids":
                continue
            pubdate = info.get("pubdate", "")
            if pubdate:
                cache[pmid_str] = pubdate
                found += 1

        if batch_num % 20 == 0 or batch_num == total_batches:
            print(f"  Batch {batch_num}/{total_batches}: +{found} dates (total cached: {len(cache)})")
            save_cache(cache)

        time.sleep(DELAY)

    save_cache(cache)
    return cache


def parse_year(pubdate: str) -> int | None:
    """Extract the year from a pubdate string like '2019 Jan 15'."""
    parts = pubdate.strip().split()
    if parts:
        try:
            return int(parts[0])
        except ValueError:
            return None
    return None


def main():
    print("Extracting PMIDs from metadata files...")
    pmids = extract_pmids()
    if not pmids:
        print("No PMIDs found.")
        return

    print(f"\nTotal unique PMIDs: {len(pmids)}")

    print("\nFetching publication dates from NCBI...")
    cache = load_cache()
    cache = fetch_pubdates(list(pmids), cache)

    # Compute date range
    years = []
    dates = []
    for pmid in pmids:
        pubdate = cache.get(str(pmid))
        if pubdate:
            dates.append((pmid, pubdate))
            y = parse_year(pubdate)
            if y:
                years.append(y)

    if not years:
        print("\nNo publication dates retrieved.")
        return

    min_year = min(years)
    max_year = max(years)

    # Find the exact entries for min/max
    earliest = [(p, d) for p, d in dates if parse_year(d) == min_year]
    latest = [(p, d) for p, d in dates if parse_year(d) == max_year]

    print(f"\n{'='*50}")
    print(f"Results for {len(dates)} papers (out of {len(pmids)} PMIDs):")
    print(f"{'='*50}")
    print(f"  Earliest year: {min_year}")
    for pmid, d in earliest[:5]:
        print(f"    PMID {pmid}: {d}")
    if len(earliest) > 5:
        print(f"    ... and {len(earliest)-5} more")

    print(f"  Latest year:   {max_year}")
    for pmid, d in latest[:5]:
        print(f"    PMID {pmid}: {d}")
    if len(latest) > 5:
        print(f"    ... and {len(latest)-5} more")

    print(f"\n  Publication year range: {min_year} - {max_year}")
    print(f"  Papers with dates retrieved: {len(dates)}/{len(pmids)}")

    # Year distribution summary
    from collections import Counter
    year_counts = Counter(years)
    print(f"\n  Year distribution (top 10):")
    for y, c in year_counts.most_common(10):
        print(f"    {y}: {c} papers")


if __name__ == "__main__":
    main()
