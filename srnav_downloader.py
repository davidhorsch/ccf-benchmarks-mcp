#!/usr/bin/env python3
"""
SRNAV CSRD Report Downloader
Downloads CSRD sustainability reports from srnav.com for benchmark analysis.
Targets manufacturing, logistics, energy, and chemical sectors.

No login required — report index and PDFs are publicly accessible.

Usage:
    python srnav_downloader.py
    python srnav_downloader.py --max 10 --dry-run
    python srnav_downloader.py --output /path/to/output
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
    "Resource Transformation",            # Chemicals, materials
    "Transportation",                     # Automotive, logistics
    "Extractives & Minerals Processing",  # Oil & gas, mining
    "Infrastructure",                     # Energy & utilities
    "Food & Beverage",                    # Cross-sector comparison
]

PRIORITY_INDUSTRIES = [
    "Chemicals",
    "Logistics",
    "Road Transportation",
    "Oil & Gas",
    "Electric Utilities & Power Generators",
    "Automobiles",
    "Industrial Machinery & Goods",
    "Food Processing",
]

PREFERRED_COUNTRIES = [
    "Germany", "France", "Netherlands", "Belgium", "Austria",
    "Switzerland", "Sweden", "Denmark", "Norway",
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
    company = doc.get("company") or {}
    sector   = company.get("sector")   or ""
    industry = company.get("industry") or ""
    country  = company.get("country")  or ""

    if sector in TARGET_SECTORS:
        score += 10 + (len(TARGET_SECTORS) - TARGET_SECTORS.index(sector))

    for i, ind in enumerate(PRIORITY_INDUSTRIES):
        if ind.lower() in industry.lower():
            score += 5 + (len(PRIORITY_INDUSTRIES) - i)
            break

    if country in PREFERRED_COUNTRIES:
        score += 3

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
            c = pool[pos[s]]; pos[s] += 1
            if c["id"] not in seen:
                selected.append(c); seen.add(c["id"]); break

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

def download_pdf(doc: dict, output_dir: Path) -> Optional[Path]:
    company = doc.get("company") or {}
    name = (company.get("name") or "unknown").replace("/", "-").replace(" ", "_")
    year = doc.get("year", "unknown")
    filepath = output_dir / f"{name}_{year}_CSRD.pdf"

    if filepath.exists():
        print(f"  [skip] {filepath.name} already exists")
        return filepath

    url = doc["original_link"]
    print(f"  [↓] {name} ({year})  {url[:90]}...")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=60, stream=True)
        resp.raise_for_status()

        if "html" in resp.headers.get("content-type", "").lower():
            print(f"  [warn] Got HTML instead of PDF — skipping {name}")
            return None

        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"  [ok] {filepath.name} ({filepath.stat().st_size // 1024} KB)")
        return filepath

    except requests.RequestException as e:
        print(f"  [error] {name}: {e}")
        return None


# ---------------------------------------------------------------------------
# METADATA CSV
# ---------------------------------------------------------------------------

def save_metadata(selected: list, output_dir: Path) -> None:
    path = output_dir / "metadata.csv"
    fields = [
        "company_name", "sector", "industry", "country", "year",
        "csrd_compliant", "csrd_report_number", "auditor",
        "pdfpage_sust_start", "pdfpage_sust_end", "publication_date",
        "original_link", "lei", "isin", "local_file",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for doc in selected:
            c = doc.get("company") or {}
            w.writerow({
                "company_name":       c.get("name", ""),
                "sector":             c.get("sector", ""),
                "industry":           c.get("industry", ""),
                "country":            c.get("country", ""),
                "year":               doc.get("year", ""),
                "csrd_compliant":     doc.get("csrd_compliant", ""),
                "csrd_report_number": doc.get("csrd_report_number", ""),
                "auditor":            doc.get("auditor", ""),
                "pdfpage_sust_start": doc.get("pdfpage_sust_start", ""),
                "pdfpage_sust_end":   doc.get("pdfpage_sust_end", ""),
                "publication_date":   doc.get("publication_date", ""),
                "original_link":      doc.get("original_link", ""),
                "lei":                c.get("lei", ""),
                "isin":               c.get("isin", ""),
                "local_file":         doc.get("_local_path", ""),
            })
    print(f"[meta] Saved {path}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Download CSRD reports from srnav.com (no login required)")
    parser.add_argument("--max",     type=int, default=10,           help="Max companies to download (default: 10)")
    parser.add_argument("--output",  default=str(OUTPUT_DIR),        help="Output directory for PDFs")
    parser.add_argument("--dry-run", action="store_true",            help="Show selection without downloading")
    args = parser.parse_args()

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        documents = fetch_all_reports()
    except Exception as e:
        print(f"[error] {e}")
        sys.exit(1)

    selected = select_companies(documents, args.max)

    print(f"\n{'#':<3} {'Company':<35} {'Sector':<32} {'Industry':<32} {'Country':<12} Year")
    print("─" * 120)
    for i, doc in enumerate(selected, 1):
        c = doc.get("company") or {}
        print(f"{i:<3} {c.get('name',''):<35} {c.get('sector',''):<32} {c.get('industry',''):<32} {c.get('country',''):<12} {doc.get('year','')}")

    if args.dry_run:
        print("\n[dry-run] No files downloaded.")
        save_metadata(selected, output_dir)
        return

    print(f"\n[download] {len(selected)} PDFs → {output_dir}/")
    downloaded = []
    for doc in selected:
        fp = download_pdf(doc, output_dir)
        if fp:
            doc["_local_path"] = str(fp)
            downloaded.append(doc)
        time.sleep(1.0)

    save_metadata(selected, output_dir)
    print(f"\n[done] {len(downloaded)}/{len(selected)} reports downloaded to {output_dir}/")


if __name__ == "__main__":
    main()
