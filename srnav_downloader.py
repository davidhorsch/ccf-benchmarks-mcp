#!/usr/bin/env python3
"""
SRNAV CSRD Report Downloader
Downloads CSRD sustainability reports from srnav.com for benchmark analysis.
Targets technology, manufacturing, food, transport, infrastructure, consumer goods, and extractives sectors.

No login required — report index and PDFs are publicly accessible.

Usage:
    python srnav_downloader.py
    python srnav_downloader.py --max 10 --dry-run
    python srnav_downloader.py --output /path/to/output

Example Run Command from terminal for 20 reports:
cd "/Users/davidhorsch/Claude/0.1 GitRepo/ccf-benchmarks-mcp" && .venv/bin/python srnav_downloader.py --max 20 2>&1)
"""

import argparse
import csv
import sys
import time
from pathlib import Path
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

BASE_URL = "https://www.srnav.com"

TARGET_SECTORS = [
    "Technology & Communications",        # Software, semiconductors, telecom
    "Resource Transformation",            # Chemicals, materials
    "Food & Beverage",                    # Processed foods, beverages, retail
    "Transportation",                     # Automotive, logistics
    "Infrastructure",                     # Energy & utilities
    "Consumer Goods",                     # Apparel, household, retail
    "Extractives & Minerals Processing",  # Oil & gas, mining
]

PRIORITY_INDUSTRIES = [
    # Resource Transformation
    "Chemicals",
    "Electrical & Electronic Equipment",
    "Industrial Machinery & Goods",
    "Building Products & Furnishings",
    "Pulp & Paper Products",
    "Construction Materials",
    # Transportation
    "Auto Parts",
    "Automobiles",
    "Air Freight & Logistics",
    "Marine Transportation",
    "Aerospace & Defence",
    "Rail Transportation",
    # Infrastructure
    "Electric Utilities & Power Generators",
    "Engineering & Construction Services",
    # Technology & Communications
    "Semiconductors",
    "Hardware",
    # Consumer Goods
    "Apparel, Accessories & Footwear",
    "Household & Personal Products",
    "Media & Entertainment",
    "Multiline and Specialty Retailers & Distributors",
    "Toys & Sporting Goods",
    # Extractives & Minerals Processing
    "Metals & Mining",
    "Oil & Gas - Refining & Marketing",
    "Oil & Gas - Exploration & Production",
    # Food & Beverage
    "Processed Foods",
    "Alcoholic Beverages",
    "Food Retailers & Distributors",
]

OUTPUT_DIR = Path(__file__).parent / "csrd_reports"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "application/json, */*",
}

# ---------------------------------------------------------------------------
# SVELTEKIT DATA PARSER
# ---------------------------------------------------------------------------

def resolve_val(data_array: list, value):
    """Resolve one level of SvelteKit index references. Booleans/None are literals."""
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int) and 0 <= value < len(data_array):
        target = data_array[value]
        if not isinstance(target, bool) and isinstance(target, int):
            return data_array[target] if 0 <= target < len(data_array) else target
        return target
    return value


def extract_document(data_array: list, doc_idx: int) -> Optional[dict]:
    """
    Extract a single document from the SvelteKit data array.
    Only resolves the fields we need — avoids circular refs in the stats object.
    """
    doc_raw = data_array[doc_idx]
    if not isinstance(doc_raw, dict):
        return None

    rv = resolve_val

    company_raw = rv(data_array, doc_raw.get("company"))
    company: dict = {}
    if isinstance(company_raw, dict):
        company = {
            "id":       rv(data_array, company_raw.get("id")),
            "lei":      rv(data_array, company_raw.get("lei")),
            "isin":     rv(data_array, company_raw.get("isin")),
            "name":     rv(data_array, company_raw.get("name")),
            "sector":   rv(data_array, company_raw.get("sector")),
            "country":  rv(data_array, company_raw.get("country")),
            "industry": rv(data_array, company_raw.get("industry")),
        }

    return {
        "id":                 rv(data_array, doc_raw.get("id")),
        "year":               rv(data_array, doc_raw.get("year")),
        "type":               rv(data_array, doc_raw.get("type")),
        "csrd_compliant":     rv(data_array, doc_raw.get("csrd_compliant")),
        "csrd_report_number": rv(data_array, doc_raw.get("csrd_report_number")),
        "active":             rv(data_array, doc_raw.get("active")),
        "pdfpage_sust_start": rv(data_array, doc_raw.get("pdfpage_sust_start")),
        "pdfpage_sust_end":   rv(data_array, doc_raw.get("pdfpage_sust_end")),
        "original_link":      rv(data_array, doc_raw.get("original_link")),
        "publication_date":   rv(data_array, doc_raw.get("publication_date")),
        "auditor":            rv(data_array, doc_raw.get("auditor")),
        "company":            company,
    }


def fetch_all_reports() -> list:
    """Fetch and parse all reports from the public SvelteKit __data.json endpoint."""
    print("[fetch] Loading reports index from srnav.com...")

    resp = requests.get(f"{BASE_URL}/reports/__data.json", headers=HEADERS, timeout=30)
    resp.raise_for_status()

    nodes = resp.json().get("nodes", [])
    if len(nodes) < 2:
        raise ValueError("Unexpected __data.json structure")

    data_array = nodes[1].get("data", [])
    root = data_array[0] if data_array else {}
    if not isinstance(root, dict) or "documents" not in root:
        raise ValueError(f"Unexpected root node: {root}")

    doc_indices = data_array[root["documents"]]
    if not isinstance(doc_indices, list):
        raise ValueError("Document index list not found")

    documents = []
    for idx in doc_indices:
        try:
            doc = extract_document(data_array, idx)
            if doc and doc.get("id"):
                documents.append(doc)
        except Exception:
            continue

    print(f"[fetch] Found {len(documents)} reports.")
    return documents


# ---------------------------------------------------------------------------
# SELECTION
# ---------------------------------------------------------------------------

def score_document(doc: dict) -> int:
    score = 0
    company  = doc.get("company") or {}
    sector   = company.get("sector")   or ""
    industry = company.get("industry") or ""

    if sector in TARGET_SECTORS:
        score += 10 + (len(TARGET_SECTORS) - TARGET_SECTORS.index(sector))

    for i, ind in enumerate(PRIORITY_INDUSTRIES):
        if ind.lower() in industry.lower():
            score += 5 + (len(PRIORITY_INDUSTRIES) - i)
            break

    if doc.get("csrd_compliant") == "full":
        score += 2

    try:
        pages = int(doc.get("pdfpage_sust_end") or 0) - int(doc.get("pdfpage_sust_start") or 0)
        score += min(pages // 20, 5)
    except (ValueError, TypeError):
        pass

    return score


def select_companies(documents: list, max_companies: int) -> list:
    """
    Select top N companies with sector diversity.
    One report per company. Round-robin across target sectors first,
    then fill remaining slots with best-scored from any sector.
    """
    docs = [d for d in documents if d.get("original_link") and d.get("company")]
    for d in docs:
        d["_score"] = score_document(d)

    # One report per company (best score wins)
    by_company: dict = {}
    for doc in docs:
        cid = (doc.get("company") or {}).get("id") or doc["id"]
        if cid not in by_company or doc["_score"] > by_company[cid]["_score"]:
            by_company[cid] = doc

    # Group into sector pools
    sector_pools: dict = {}
    for doc in by_company.values():
        s = (doc.get("company") or {}).get("sector") or "Other"
        sector_pools.setdefault(s, []).append(doc)
    for pool in sector_pools.values():
        pool.sort(key=lambda d: d["_score"], reverse=True)

    ordered = [s for s in TARGET_SECTORS if s in sector_pools]
    ordered += [s for s in sector_pools if s not in TARGET_SECTORS]
    pos = {s: 0 for s in ordered}

    selected, seen = [], set()

    # Pass 1: one per sector
    for s in ordered:
        if len(selected) >= max_companies:
            break
        pool = sector_pools[s]
        while pos[s] < len(pool):
            c = pool[pos[s]]
            pos[s] += 1
            if c["id"] not in seen:
                selected.append(c)
                seen.add(c["id"])
                break

    # Pass 2: fill remaining with best-scored
    if len(selected) < max_companies:
        rest = sorted(
            [d for d in by_company.values() if d["id"] not in seen],
            key=lambda d: d["_score"], reverse=True
        )
        for doc in rest:
            if len(selected) >= max_companies:
                break
            selected.append(doc)

    return selected[:max_companies]


# ---------------------------------------------------------------------------
# DOWNLOAD
# ---------------------------------------------------------------------------

def _safe_unlink(path: Path) -> None:
    """Delete a file only if it is empty (0 bytes). Raises if the file has content."""
    if path.exists() and path.stat().st_size > 0:
        raise RuntimeError(
            f"Refusing to delete non-empty file {path.name} ({path.stat().st_size} bytes). "
            "Only 0-byte or partial files may be removed."
        )
    path.unlink(missing_ok=True)


RETRYABLE_STATUS = {429, 503}
MAX_RETRIES = 3
RETRY_BACKOFF = 5  # seconds; doubles each attempt (5 → 10 → 20)


def download_pdf(doc: dict, output_dir: Path) -> Optional[Path]:
    company = doc.get("company") or {}
    name = (company.get("name") or "unknown").replace("/", "-").replace(" ", "_")
    year = doc.get("year", "unknown")
    filepath = output_dir / f"{name}_{year}_CSRD.pdf"

    if filepath.exists():
        if filepath.stat().st_size > 0:
            print(f"  [skip] {filepath.name} already exists")
            return filepath
        _safe_unlink(filepath)  # stale 0-byte file from a prior failed download — retry

    url = doc["original_link"]
    print(f"  [↓] {name} ({year})  {url[:90]}...")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=60, stream=True)

            if resp.status_code in RETRYABLE_STATUS and attempt < MAX_RETRIES:
                wait = int(resp.headers.get("Retry-After") or RETRY_BACKOFF * (2 ** (attempt - 1)))
                print(f"  [retry] {name}: HTTP {resp.status_code} — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            resp.raise_for_status()

            if "html" in resp.headers.get("content-type", "").lower():
                print(f"  [warn] Got HTML instead of PDF — skipping {name}")
                return None

            with open(filepath, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

            if filepath.stat().st_size == 0:
                _safe_unlink(filepath)
                print(f"  [warn] {name}: server returned an empty file — skipping")
                return None

            print(f"  [ok] {filepath.name} ({filepath.stat().st_size // 1024} KB)")
            return filepath

        except requests.RequestException as e:
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * (2 ** (attempt - 1))
                print(f"  [retry] {name}: {e} — waiting {wait}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
            else:
                print(f"  [error] {name}: {e}")
                _safe_unlink(filepath)  # clean up any partial write

    return None


# ---------------------------------------------------------------------------
# METADATA CSV
# ---------------------------------------------------------------------------

# Maps our metadata company names (lowercase) to the name srnav uses (lowercase).
# Needed so the "already present" check recognises the same company under both names.
_COMPANY_ALIASES: dict[str, str] = {
    "aperam":         "aperam sa",
    "bmw group":      "bmw",
    "dsv":            "dsv panalpina",
    "prosiebensat.1": "prosiebensat.1 media",
    "publicis":       "publicis groupe",
    "renault group":  "renault",
    "thyssenkrupp":   "thyssenkrupp ag",
}

# Column order matches csrd_reports/metadata.csv used by extract_kpis.py
METADATA_FIELDS = [
    "company_name", "sector", "sub_sector", "country", "year",
    "pdfpage_sust_start", "pdfpage_sust_end", "local_file",
    "csrd_compliant", "auditor", "publication_date",
    "original_link", "lei", "isin",
]


def _load_existing_metadata(output_dir: Path) -> tuple[list[dict], set[tuple[str, str]]]:
    """Read metadata.csv and return (rows, {(company_lower, year_str)}).

    Skips comment lines (e.g. spreadsheet annotations starting with '#').
    """
    path = output_dir / "metadata.csv"
    if not path.exists():
        return [], set()
    lines = [l for l in path.read_text(encoding="utf-8").splitlines()
             if not l.startswith("#")]
    rows = list(csv.DictReader(lines))
    seen: set[tuple[str, str]] = set()
    for r in rows:
        if not (r.get("company_name") and r.get("year")):
            continue
        name = r["company_name"].strip().lower()
        year = str(r["year"]).strip()
        seen.add((name, year))
        # Also register the srnav variant so we don't re-download the same company
        if name in _COMPANY_ALIASES:
            seen.add((_COMPANY_ALIASES[name], year))
    return rows, seen


def _doc_to_row(doc: dict) -> dict:
    """Convert a srnav document dict to a metadata.csv row."""
    c = doc.get("company") or {}
    return {
        "company_name":      c.get("name", ""),
        "sector":            c.get("sector", ""),
        "sub_sector":        c.get("industry", ""),  # srnav 'industry' → sub_sector
        "country":           c.get("country", ""),
        "year":              doc.get("year", ""),
        "pdfpage_sust_start": doc.get("pdfpage_sust_start", ""),
        "pdfpage_sust_end":  doc.get("pdfpage_sust_end", ""),
        "local_file":        doc.get("_local_path", ""),
        "csrd_compliant":    doc.get("csrd_compliant", ""),
        "auditor":           doc.get("auditor", ""),
        "publication_date":  doc.get("publication_date", ""),
        "original_link":     doc.get("original_link", ""),
        "lei":               c.get("lei", ""),
        "isin":              c.get("isin", ""),
    }


def save_metadata(new_docs: list, output_dir: Path, existing_rows: list[dict]) -> None:
    """Append new entries to metadata.csv, preserving all existing rows untouched."""
    path = output_dir / "metadata.csv"
    new_rows = [_doc_to_row(doc) for doc in new_docs]
    all_rows = existing_rows + new_rows
    assert len(all_rows) >= len(existing_rows), (
        f"BUG: metadata write would reduce row count from {len(existing_rows)} to {len(all_rows)}"
    )
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=METADATA_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(all_rows)
    print(f"[meta] {path.name} — {len(existing_rows)} kept + {len(new_rows)} added = {len(all_rows)} total")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Download CSRD reports from srnav.com (no login required)")
    parser.add_argument("--max",     type=int, default=10,           help="Max new companies to download (default: 10)")
    parser.add_argument("--output",  default=str(OUTPUT_DIR),        help="Output directory for PDFs")
    parser.add_argument("--dry-run", action="store_true",            help="Show selection without downloading or updating metadata")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    existing_rows, seen = _load_existing_metadata(output_dir)
    if seen:
        print(f"[meta] {len(seen)} companies already in metadata.csv — skipping those.")

    try:
        documents = fetch_all_reports()
    except Exception as e:
        print(f"[error] {e}")
        sys.exit(1)

    # Filter to reports not already present (matched by company name + year)
    new_documents = [
        d for d in documents
        if (
            (d.get("company") or {}).get("name", "").lower(),
            str(d.get("year", ""))
        ) not in seen
    ]
    already = len(documents) - len(new_documents)
    print(f"[filter] {len(documents)} available on srnav — {already} already present, {len(new_documents)} new candidates.")

    # Hard filter: only keep reports from target sectors
    new_documents = [
        d for d in new_documents
        if (d.get("company") or {}).get("sector") in TARGET_SECTORS
    ]
    print(f"[filter] {len(new_documents)} candidates after restricting to target sectors.")

    if not new_documents:
        print("[done] All reports already downloaded — nothing to do.")
        return

    selected = select_companies(new_documents, args.max)

    print(f"\n{'#':<3} {'Company':<35} {'Sector':<32} {'Industry':<32} {'Country':<12} Year")
    print("─" * 120)
    for i, doc in enumerate(selected, 1):
        c = doc.get("company") or {}
        print(f"{i:<3} {c.get('name',''):<35} {c.get('sector',''):<32} {c.get('industry',''):<32} {c.get('country',''):<12} {doc.get('year','')}")

    if args.dry_run:
        print(f"\n[dry-run] Would download {len(selected)} new reports — metadata not updated.")
        return

    print(f"\n[download] {len(selected)} new PDFs → {output_dir}/")
    existing_files = {r.get("local_file", "") for r in existing_rows}
    downloaded = []
    for doc in selected:
        fp = download_pdf(doc, output_dir)
        if fp and fp.name not in existing_files:
            doc["_local_path"] = fp.name   # filename only, matching metadata.csv convention
            downloaded.append(doc)
        time.sleep(1.0)

    if downloaded:
        save_metadata(downloaded, output_dir, existing_rows)
    print(f"\n[done] {len(downloaded)}/{len(selected)} new reports downloaded to {output_dir}/")


if __name__ == "__main__":
    main()
