#!/usr/bin/env python3
"""
CSRD KPI Extractor — Gemini edition

Uploads each CSRD PDF to the Google Files API and uses Gemini 2.5 Flash
to extract GHG emissions (Scope 1/2/3), revenue, and FTE directly from the
PDF (no text pre-processing needed). Results are written into
industry_benchmarks.json as empirical data points.

Usage:
    python extract_kpis.py
    python extract_kpis.py --dry-run     # print results without writing JSON
    python extract_kpis.py --company BASF  # run a single company by name
"""

import argparse
import csv
import json
import os
import re
import time
from pathlib import Path

from dotenv import load_dotenv, find_dotenv
from google import genai
from google.genai import types

load_dotenv(find_dotenv())

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

GOOGLE_API_KEY = os.environ["GOOGLE_API_KEY"]
MODEL          = "gemini-2.5-flash"
REPORTS_DIR    = Path(__file__).parent / "csrd_reports"
BENCHMARKS     = Path(__file__).parent / "industry_benchmarks.json"

# ---------------------------------------------------------------------------
# REPORTS — loaded from csrd_reports/metadata.csv
# ---------------------------------------------------------------------------

def _load_reports() -> list[dict]:
    """Load the report list from csrd_reports/metadata.csv.

    The CSV is the single source of truth for which PDFs to process.
    Add or remove rows there to control what gets extracted.
    """
    csv_path = REPORTS_DIR / "metadata.csv"
    if not csv_path.exists():
        print(f"[warn] metadata.csv not found at {csv_path} — REPORTS will be empty.")
        return []
    raw = csv_path.read_text(encoding="utf-8")
    lines = [l for l in raw.splitlines() if not l.startswith("#")]
    reports = []
    for row in csv.DictReader(lines):
        if not row.get("local_file"):
            continue
        try:
            reports.append({
                "file":          row["local_file"],
                "company":       row["company_name"],
                "sector":        row["sector"],
                "sub_sector":    row["sub_sector"],
                "country":       row["country"],
                "year":          int(row["year"]),
                "sust_start":    int(row["pdfpage_sust_start"]),
                "sust_end":      int(row["pdfpage_sust_end"]),
                "original_link": row.get("original_link") or "",
            })
        except (ValueError, KeyError) as exc:
            print(f"[warn] Skipping malformed row in metadata.csv: {exc}")
            continue
    return reports


REPORTS = _load_reports()

# ---------------------------------------------------------------------------
# SUB-SECTOR NORMALIZATION
# ---------------------------------------------------------------------------

# Canonical sub_sector names. Any variant that appears in REPORTS or in the
# JSON (e.g. hand-edited specialisations) is mapped to the canonical form here
# so that sector filtering in the MCP server works consistently.
SUB_SECTOR_ALIASES: dict[str, str] = {
    "Chemical Industry (Industrial Gases)":    "Chemical Industry",
    "Chemical Industry (Fibers)":              "Chemical Industry",
    "Chemical Industry (Cat 11 S3)":           "Chemical Industry",
    "Industrial Machinery & Equipment (Wind)": "Industrial Machinery & Equipment",
    "Oil & Gas (Refining & Marketing)":        "Oil & Gas",
}


def normalize_sub_sector(sub_sector: str) -> str:
    return SUB_SECTOR_ALIASES.get(sub_sector, sub_sector)


EXTRACTION_PROMPT = """\
You are a sustainability data analyst. Extract the following KPIs from this \
CSRD sustainability report. Focus on pages {sust_start}–{sust_end} where the \
sustainability disclosures are located, but check nearby pages if a value is \
referenced there.

Return ONLY a valid JSON object — no explanation, no markdown, no code fences. \
Use null for any value not found.

Required fields:
{{
  "scope1_tco2e": <number or null>,
  "scope2_lb_tco2e": <number or null>,       // location-based Scope 2
  "scope2_mb_tco2e": <number or null>,       // market-based Scope 2 (preferred)
  "scope2_tco2e": <number or null>,          // use ONLY if LB/MB split is not disclosed
  "scope3_total_tco2e": <number or null>,
  "scope3_by_category": {{                   // fill every category that is disclosed; null if not reported
    "cat1_purchased_goods_services": <number or null>,
    "cat2_capital_goods": <number or null>,
    "cat3_fuel_energy_activities": <number or null>,
    "cat4_upstream_transport": <number or null>,
    "cat5_waste_in_operations": <number or null>,
    "cat6_business_travel": <number or null>,
    "cat7_employee_commuting": <number or null>,
    "cat8_upstream_leased_assets": <number or null>,
    "cat9_downstream_transport": <number or null>,
    "cat10_processing_of_sold_products": <number or null>,
    "cat11_use_of_sold_products": <number or null>,
    "cat12_end_of_life_treatment": <number or null>,
    "cat13_downstream_leased_assets": <number or null>,
    "cat14_franchises": <number or null>,
    "cat15_investments": <number or null>
  }},
  "revenue_eur_million": <number or null>,   // convert to EUR million if needed
  "fte": <number or null>,                   // total headcount or FTE
  "revenue_currency_original": "<string>",   // e.g. "EUR", "USD", "DKK"
  "revenue_original_value": <number or null>,
  "reporting_year": <number or null>,
  "notes": "<ALWAYS begin with source pages: 'GHG emissions: p.X; Revenue: p.Y; FTE: p.Z' — then add caveats, unit conversions, ambiguities>"
}}

Rules:
- All emissions in tCO2e (convert: 1 kt = 1,000 t; 1 Mt = 1,000,000 t)
- Revenue in EUR million (convert: 1 DKK = 0.134; 1 SEK = 0.087; 1 NOK = 0.086; \
1 GBP = 1.17; 1 USD = 0.92; 1 CHF = 1.05 — multiply original value × rate)
- For FTE: use total headcount or FTE, not part-time equivalents alone
- Prefer market-based Scope 2 for intensity; fall back to location-based
- If Scope 3 total is not stated but categories are present, sum them
- "CO2e", "CO₂e", "GHG", "greenhouse gas" all refer to the same metric
- Revenue is often found in EU Taxonomy tables or the financial highlights section
- Ensure the reporting data refers to an entire calendar year. Check https://en.wikipedia.org/wiki/Fiscal_year for reference and highlight if data referes to fiscal year if calendar year not available in the notes.
- notes MUST start with page references for every extracted value before any other text
"""


# ---------------------------------------------------------------------------
# GEMINI EXTRACTION
# ---------------------------------------------------------------------------

def upload_pdf(client: genai.Client, pdf_path: Path) -> types.File:
    """Upload a PDF to the Google Files API and return the File object."""
    print(f"  [upload] {pdf_path.name} ({pdf_path.stat().st_size // 1024:,} KB)...")
    with open(pdf_path, "rb") as f:
        uploaded = client.files.upload(
            file=f,
            config=types.UploadFileConfig(
                mime_type="application/pdf",
                display_name=pdf_path.name,
            ),
        )
    # Wait until the file is fully processed
    for _ in range(30):
        file_info = client.files.get(name=uploaded.name)
        if file_info.state == types.FileState.ACTIVE:
            break
        if file_info.state == types.FileState.FAILED:
            raise RuntimeError(f"File processing failed: {pdf_path.name}")
        time.sleep(2)
    else:
        raise RuntimeError(f"File upload timed out: {pdf_path.name}")
    return uploaded


def extract_kpis_with_gemini(
    client: genai.Client,
    uploaded_file: types.File,
    company: str,
    sust_start: int,
    sust_end: int,
    max_retries: int = 3,
) -> dict:
    """Ask Gemini to extract KPIs from the uploaded PDF and return parsed JSON."""
    prompt = EXTRACTION_PROMPT.format(
        sust_start=sust_start,
        sust_end=sust_end,
        company=company,
    )

    for attempt in range(1, max_retries + 1):
        try:
            response = client.models.generate_content(
                model=MODEL,
                contents=[
                    types.Part.from_uri(
                        file_uri=uploaded_file.uri,
                        mime_type="application/pdf",
                    ),
                    prompt,
                ],
                config=types.GenerateContentConfig(
                    temperature=0,
                    max_output_tokens=8192,
                ),
            )
            break  # success
        except Exception as e:
            status = getattr(e, "status_code", None) or getattr(e, "code", None)
            wait = 60 if status == 429 else 15
            if attempt < max_retries:
                print(f"  [retry {attempt}/{max_retries}] {e.__class__.__name__} — waiting {wait}s...")
                time.sleep(wait)
            else:
                print(f"  [error] {e.__class__.__name__}: {str(e)[:120]}")
                return {}

    raw = response.text.strip() if response.text else ""

    # Strip markdown fences if present
    raw = re.sub(r"^```(?:json)?\n?", "", raw)
    raw = re.sub(r"\n?```$", "", raw)

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        # Attempt to recover a truncated JSON by extracting completed key:value pairs
        recovered = {}
        for m in re.finditer(r'"(\w+)"\s*:\s*(-?\d+(?:\.\d+)?|null|true|false|"[^"]*")', raw):
            key, val_str = m.group(1), m.group(2)
            if val_str == "null":
                recovered[key] = None
            elif val_str in ("true", "false"):
                recovered[key] = val_str == "true"
            elif val_str.startswith('"'):
                recovered[key] = val_str.strip('"')
            else:
                recovered[key] = float(val_str) if "." in val_str else int(val_str)
        if recovered:
            print(f"  [warn] Truncated JSON for {company} — recovered {len(recovered)} fields via fallback")
            return recovered
        print(f"  [warn] JSON parse error for {company}: {e}")
        print(f"  Raw response: {raw[:300]}")
        return {}


def delete_file(client: genai.Client, uploaded_file: types.File) -> None:
    """Delete an uploaded file from the Files API to free quota."""
    try:
        client.files.delete(name=uploaded_file.name)
    except Exception:
        pass  # Non-fatal


# ---------------------------------------------------------------------------
# INTENSITY CALCULATION
# ---------------------------------------------------------------------------

def compute_intensities(kpis: dict) -> dict:
    """Calculate tCO2e/EUR M and tCO2e/FTE intensity metrics."""
    s1  = kpis.get("scope1_tco2e")
    # Use explicit None checks so that 0 (e.g. 100 % renewable via RECs) is
    # treated as a valid value and not silently skipped by a falsy `or` chain.
    s2  = (
        kpis.get("scope2_mb_tco2e")
        if kpis.get("scope2_mb_tco2e") is not None
        else kpis.get("scope2_lb_tco2e")
        if kpis.get("scope2_lb_tco2e") is not None
        else kpis.get("scope2_tco2e")
    )
    s3  = kpis.get("scope3_total_tco2e")
    rev = kpis.get("revenue_eur_million")
    fte = kpis.get("fte")

    result = {}

    if s1 is not None and s2 is not None:
        s12 = s1 + s2
        result["scope1_tco2e"]  = s1
        result["scope2_tco2e"]  = s2
        result["scope12_tco2e"] = s12
        if rev and rev > 0:
            result["intensity_s12_per_eur_m"] = round(s12 / rev, 1)
        if fte and fte > 0:
            result["intensity_s12_per_fte"]   = round(s12 / fte, 1)

    if s3 is not None:
        result["scope3_tco2e"] = s3
        if s1 is not None and s2 is not None:
            s123 = s1 + s2 + s3
            result["scope123_tco2e"] = s123
            if rev and rev > 0:
                result["intensity_s123_per_eur_m"] = round(s123 / rev, 1)
            if fte and fte > 0:
                result["intensity_s123_per_fte"]   = round(s123 / fte, 1)

    if rev:
        result["revenue_eur_million"] = rev
    if fte:
        result["fte"] = fte

    return result


# ---------------------------------------------------------------------------
# JSON UPDATE
# ---------------------------------------------------------------------------

def _build_id(company: str, year: int) -> str:
    return f"csrd-{company.lower().replace(' ', '-')}-{year}"


def build_benchmark_entry(report: dict, kpis: dict, intensities: dict) -> dict:
    """Build a benchmark entry dict for industry_benchmarks.json."""
    return {
        "id": _build_id(report["company"], report["year"]),
        "source_type": "csrd_report",
        "company":    report["company"],
        "sector":     report["sector"],
        "sub_sector": normalize_sub_sector(report["sub_sector"]),
        "country":    report["country"],
        "year":       report["year"],
        "raw_kpis": {
            "scope1_tco2e":              kpis.get("scope1_tco2e"),
            "scope2_lb_tco2e":           kpis.get("scope2_lb_tco2e"),
            "scope2_mb_tco2e":           kpis.get("scope2_mb_tco2e"),
            "scope3_total_tco2e":        kpis.get("scope3_total_tco2e"),
            "scope3_by_category":        kpis.get("scope3_by_category"),
            "revenue_eur_million":       kpis.get("revenue_eur_million"),
            "revenue_currency_original": kpis.get("revenue_currency_original"),
            "revenue_original_value":    kpis.get("revenue_original_value"),
            "fte":                       kpis.get("fte"),
            "reporting_year":            kpis.get("reporting_year"),
            "notes":                     kpis.get("notes"),
        },
        "intensities": intensities,
        "confidence": "high" if intensities.get("intensity_s12_per_eur_m") else "low",
        "document":   report.get("original_link") or report["file"],
    }


def update_benchmarks_json(new_entries: list, dry_run: bool, silent: bool = False) -> None:
    """Upsert CSRD entries into industry_benchmarks.json."""
    with open(BENCHMARKS, "r", encoding="utf-8") as f:
        db = json.load(f)

    db.setdefault("csrd_company_data", [])
    existing_ids = {e["id"]: i for i, e in enumerate(db["csrd_company_data"])}

    for entry in new_entries:
        idx = existing_ids.get(entry["id"])
        if idx is not None:
            db["csrd_company_data"][idx] = entry
        else:
            db["csrd_company_data"].append(entry)

    if dry_run:
        print("\n[dry-run] Would write to industry_benchmarks.json:")
        print(json.dumps({"csrd_company_data": new_entries}, indent=2, ensure_ascii=False))
        return

    with open(BENCHMARKS, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)
    if not silent:
        print(f"\n[done] Updated {BENCHMARKS} with {len(new_entries)} CSRD entries.")


# ---------------------------------------------------------------------------
# MIGRATION
# ---------------------------------------------------------------------------

def migrate_benchmarks(dry_run: bool = False) -> None:
    """Re-apply script rules to all existing entries without calling Gemini.

    Applies:
      - sub_sector normalisation via SUB_SECTOR_ALIASES
      - scope3_by_category added to raw_kpis (null if not captured)
      - reporting_year added to raw_kpis if missing
      - intensities rebuilt from raw_kpis via compute_intensities()
      - confidence recalculated
    """
    with open(BENCHMARKS, "r", encoding="utf-8") as f:
        db = json.load(f)

    entries = db.get("csrd_company_data", [])
    change_log: list[str] = []

    # Build lookup: company name → source PDF filename (from REPORTS list)
    company_to_file: dict[str, str] = {r["company"]: r["file"] for r in REPORTS}

    for entry in entries:
        company = entry["company"]
        raw     = entry.setdefault("raw_kpis", {})
        log: list[str] = []

        # 1. Normalize sub_sector
        old_sub = entry["sub_sector"]
        new_sub = normalize_sub_sector(old_sub)
        if new_sub != old_sub:
            entry["sub_sector"] = new_sub
            log.append(f"sub_sector: '{old_sub}' → '{new_sub}'")

        # 2. Add document filename if missing
        if "document" not in entry:
            doc = company_to_file.get(company)
            entry["document"] = doc  # None for hand-authored entries not in REPORTS
            log.append(f"document: '{doc}'")

        # 3. Ensure scope3_by_category in raw_kpis
        if "scope3_by_category" not in raw:
            raw["scope3_by_category"] = None
            log.append("raw_kpis: added scope3_by_category=null")

        # 4. Ensure reporting_year in raw_kpis
        if "reporting_year" not in raw:
            raw["reporting_year"] = None
            log.append("raw_kpis: added reporting_year=null")

        # 5. Rebuild intensities from raw_kpis
        new_intensities = compute_intensities(raw)
        if entry.get("intensities") != new_intensities:
            entry["intensities"] = new_intensities
            log.append("intensities: rebuilt from raw_kpis")

        # 6. Recalculate confidence
        new_conf = "high" if new_intensities.get("intensity_s12_per_eur_m") else "low"
        if entry.get("confidence") != new_conf:
            log.append(f"confidence: '{entry.get('confidence')}' → '{new_conf}'")
            entry["confidence"] = new_conf

        if log:
            change_log.append(f"  {company}: " + "; ".join(log))

    if change_log:
        print(f"\n[migrate] {len(change_log)} entries updated:")
        for line in change_log:
            print(line)
    else:
        print("[migrate] No changes needed.")

    if dry_run:
        print("\n[dry-run] JSON not written.")
        return

    with open(BENCHMARKS, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)
    print(f"\n[migrate] Written → {BENCHMARKS}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Extract CSRD KPIs using Gemini 2.5 Flash")
    parser.add_argument("--dry-run",  action="store_true", help="Print results without writing JSON / downloading")
    parser.add_argument("--company",  default=None,        help="Run only this company (substring match)")
    parser.add_argument("--migrate",  action="store_true", help="Apply script rules to existing JSON entries without calling Gemini")
    parser.add_argument("--force",    action="store_true", help="Re-extract companies already in the JSON (overwrite)")
    args = parser.parse_args()

    if args.migrate:
        migrate_benchmarks(dry_run=args.dry_run)
        return

    client = genai.Client(api_key=GOOGLE_API_KEY)

    reports = REPORTS
    if args.company:
        reports = [r for r in REPORTS if args.company.lower() in r["company"].lower()]
        if not reports:
            print(f"[error] No company matching '{args.company}'")
            return

    # Skip reports already present in the JSON unless --force
    existing_ids: set[str] = set()
    if BENCHMARKS.exists() and not args.force:
        with open(BENCHMARKS, encoding="utf-8") as f:
            db = json.load(f)
        existing_ids = {e["id"] for e in db.get("csrd_company_data", [])}
        if existing_ids:
            print(f"[skip] {len(existing_ids)} companies already in {BENCHMARKS.name}")

    new_entries = []

    for report in reports:
        pdf_path = REPORTS_DIR / report["file"]
        if not pdf_path.exists():
            print(f"\n[skip] {report['file']} not found")
            continue

        if _build_id(report["company"], report["year"]) in existing_ids:
            print(f"  [skip] {report['company']} {report['year']} already extracted")
            continue

        print(f"\n[{report['company']}] pages {report['sust_start']}–{report['sust_end']}")

        uploaded_file = None
        try:
            uploaded_file = upload_pdf(client, pdf_path)

            print(f"  [extract] Sending to {MODEL}...")
            kpis = extract_kpis_with_gemini(
                client, uploaded_file,
                report["company"],
                report["sust_start"],
                report["sust_end"],
            )

            if not kpis:
                print(f"  [warn] No KPIs returned")
                continue

        finally:
            if uploaded_file:
                delete_file(client, uploaded_file)

        intensities = compute_intensities(kpis)
        entry = build_benchmark_entry(report, kpis, intensities)
        new_entries.append(entry)

        # Write after every company so a mid-run crash doesn't lose progress
        if not args.dry_run:
            update_benchmarks_json([entry], dry_run=False, silent=True)

        i = intensities
        s12  = f"{i['intensity_s12_per_eur_m']:>8.1f}"  if i.get("intensity_s12_per_eur_m")  else "       n/a"
        s123 = f"{i['intensity_s123_per_eur_m']:>8.1f}" if i.get("intensity_s123_per_eur_m") else "       n/a"
        s12f = f"{i['intensity_s12_per_fte']:>7.1f}"    if i.get("intensity_s12_per_fte")    else "      n/a"
        rev  = f"EUR {i['revenue_eur_million']:,.0f}M"  if i.get("revenue_eur_million")       else "n/a"
        fte  = f"{i['fte']:,.0f}"                       if i.get("fte")                       else "n/a"
        print(f"  S1+2/EUR M: {s12}  |  S1+2+3/EUR M: {s123}  |  S1+2/FTE: {s12f}  |  Rev: {rev}  |  FTE: {fte}")

    print(f"\n{'─' * 85}")
    print(f"Extracted {len(new_entries)}/{len(reports)} reports\n")

    print(f"{'Company':<28} {'Sector':<22} {'S1+2/EUR M':>10} {'S123/EUR M':>11} {'S1+2/FTE':>9}")
    print("─" * 85)
    for e in new_entries:
        i = e["intensities"]
        s12  = f"{i['intensity_s12_per_eur_m']:>10.1f}"  if i.get("intensity_s12_per_eur_m")  else "       n/a"
        s123 = f"{i['intensity_s123_per_eur_m']:>11.1f}" if i.get("intensity_s123_per_eur_m") else "        n/a"
        s12f = f"{i['intensity_s12_per_fte']:>9.1f}"     if i.get("intensity_s12_per_fte")    else "      n/a"
        print(f"{e['company']:<28} {e['sub_sector']:<22} {s12} {s123} {s12f}")

    update_benchmarks_json(new_entries, args.dry_run)


if __name__ == "__main__":
    main()
