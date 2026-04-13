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

# Page ranges from srnav.com metadata (sust_start / sust_end).
# Gemini reads the full PDF — these are passed as hints in the prompt so the
# model focuses on the sustainability section rather than the full annual report.
REPORTS = [
    {
        "file":       "Covestro_2025_CSRD.pdf",
        "company":    "Covestro",
        "sector":     "Manufacturing",
        "sub_sector": "Chemical Industry",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 108,
        "sust_end":   210,
    },
    {
        "file":       "BPOST_SA_2024_CSRD.pdf",
        "company":    "BPOST SA",
        "sector":     "Logistics",
        "sub_sector": "Integrated Logistics & Parcel",
        "country":    "Belgium",
        "year":       2024,
        "sust_start": 77,
        "sust_end":   230,
    },
    {
        "file":       "Rubis_2024_CSRD.pdf",
        "company":    "Rubis",
        "sector":     "Energy",
        "sub_sector": "Oil & Gas",
        "country":    "France",
        "year":       2024,
        "sust_start": 75,
        "sust_end":   257,
    },
    {
        "file":       "Ørsted_2024_CSRD.pdf",
        "company":    "Ørsted",
        "sector":     "Energy",
        "sub_sector": "Utilities (Electricity & Renewables)",
        "country":    "Denmark",
        "year":       2024,
        "sust_start": 56,
        "sust_end":   156,
    },
    {
        "file":       "Heineken_2024_CSRD.pdf",
        "company":    "Heineken",
        "sector":     "Manufacturing",
        "sub_sector": "Food & Beverage",
        "country":    "Netherlands",
        "year":       2024,
        "sust_start": 138,
        "sust_end":   285,
    },
    {
        "file":       "KPN_2024_CSRD.pdf",
        "company":    "KPN",
        "sector":     "Other",
        "sub_sector": "Telecommunications",
        "country":    "Netherlands",
        "year":       2024,
        "sust_start": 46,
        "sust_end":   162,
    },
    {
        "file":       "adidas_2024_CSRD.pdf",
        "company":    "adidas",
        "sector":     "Manufacturing",
        "sub_sector": "Consumer Goods / Apparel",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 140,
        "sust_end":   357,
    },
    {
        "file":       "Sartorius_2025_CSRD.pdf",
        "company":    "Sartorius",
        "sector":     "Manufacturing",
        "sub_sector": "Industrial Machinery & Equipment",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 111,
        "sust_end":   210,
    },
    {
        "file":       "Svenska_Handelsbanken_2024_CSRD.pdf",
        "company":    "Svenska Handelsbanken",
        "sector":     "Other",
        "sub_sector": "Financial Services",
        "country":    "Sweden",
        "year":       2024,
        "sust_start": 59,
        "sust_end":   143,
    },
    {
        "file":       "Nordex_2024_CSRD.pdf",
        "company":    "Nordex",
        "sector":     "Manufacturing",
        "sub_sector": "Industrial Machinery & Equipment",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 91,
        "sust_end":   220,
    },
    {
        "file":       "Statkraft_2025_CSRD.pdf",
        "company":    "Statkraft",
        "sector":     "Energy",
        "sub_sector": "Utilities (Electricity & Renewables)",
        "country":    "Norway",
        "year":       2024,
        "sust_start": 44,
        "sust_end":   163,
    },
    {
        "file":       "TF1_2024_CSRD.pdf",
        "company":    "TF1",
        "sector":     "Other",
        "sub_sector": "Media & Entertainment",
        "country":    "France",
        "year":       2023,
        "sust_start": 117,
        "sust_end":   220,
    },
    {
        "file":       "voestalpine_AG_2025_CSRD.pdf",
        "company":    "voestalpine AG",
        "sector":     "Manufacturing",
        "sub_sector": "Iron & Steel",
        "country":    "Austria",
        "year":       2024,
        "sust_start": 96,
        "sust_end":   350,
    },
    {
        "file":       "Air_Liquide_2024_CSRD.pdf",
        "company":    "Air Liquide",
        "sector":     "Manufacturing",
        "sub_sector": "Chemical Industry",
        "country":    "France",
        "year":       2023,
        "sust_start": 279,
        "sust_end":   382,
    },
    {
        "file":       "Lenzing_2024_CSRD.pdf",
        "company":    "Lenzing",
        "sector":     "Manufacturing",
        "sub_sector": "Chemical Industry",
        "country":    "Austria",
        "year":       2023,
        "sust_start": 100,
        "sust_end":   246,
    },
    {
        "file":       "Alzchem_Group_2025_CSRD.pdf",
        "company":    "Alzchem Group",
        "sector":     "Manufacturing",
        "sub_sector": "Chemical Industry",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 94,
        "sust_end":   242,
    },
    {
        "file":       "Evonik_2025_CSRD.pdf",
        "company":    "Evonik",
        "sector":     "Manufacturing",
        "sub_sector": "Chemical Industry",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 77,
        "sust_end":   189,
    },
    {
        "file":       "Arkema_2024_CSRD.pdf",
        "company":    "Arkema",
        "sector":     "Manufacturing",
        "sub_sector": "Chemical Industry",
        "country":    "France",
        "year":       2023,
        "sust_start": 165,
        "sust_end":   279,
    },
    {
        "file":       "Borealis_2024_CSRD.pdf",
        "company":    "Borealis",
        "sector":     "Manufacturing",
        "sub_sector": "Chemical Industry",
        "country":    "Austria",
        "year":       2023,
        "sust_start": 18,
        "sust_end":   196,
    },
    {
        "file":       "BASF_2024_CSRD.pdf",
        "company":    "BASF",
        "sector":     "Manufacturing",
        "sub_sector": "Chemical Industry",
        "country":    "Germany",
        "year":       2023,
        "sust_start": 147,
        "sust_end":   327,
    },
    {
        "file":       "Lanxess_2024_CSRD.pdf",
        "company":    "Lanxess",
        "sector":     "Manufacturing",
        "sub_sector": "Chemical Industry",
        "country":    "Germany",
        "year":       2023,
        "sust_start": 87,
        "sust_end":   224,
    },
    # --- Automotive OEM ---
    {
        "file":       "Stellantis_2024_CSRD.pdf",
        "company":    "Stellantis",
        "sector":     "Manufacturing",
        "sub_sector": "Automotive OEM",
        "country":    "Netherlands",
        "year":       2024,
        "sust_start": 1,
        "sust_end":   131,
    },
    {
        "file":       "BMW_2024_CSRD.pdf",
        "company":    "BMW Group",
        "sector":     "Manufacturing",
        "sub_sector": "Automotive OEM",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 150,
        "sust_end":   380,
    },
    # --- Oil & Gas ---
    {
        "file":       "ENI_2024_CSRD.pdf",
        "company":    "ENI",
        "sector":     "Energy",
        "sub_sector": "Oil & Gas",
        "country":    "Italy",
        "year":       2024,
        "sust_start": 1,
        "sust_end":   136,
    },
    {
        "file":       "OMV_2024_CSRD_env.pdf",
        "company":    "OMV",
        "sector":     "Energy",
        "sub_sector": "Oil & Gas",
        "country":    "Austria",
        "year":       2024,
        "sust_start": 1,
        "sust_end":   118,
    },
    {
        "file":       "TotalEnergies_2024_CSRD.pdf",
        "company":    "TotalEnergies",
        "sector":     "Energy",
        "sub_sector": "Oil & Gas",
        "country":    "France",
        "year":       2024,
        "sust_start": 300,
        "sust_end":   580,
    },
    # --- Utilities ---
    {
        "file":       "RWE_2024_full_CSRD.pdf",
        "company":    "RWE",
        "sector":     "Energy",
        "sub_sector": "Utilities (Electricity & Renewables)",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 100,
        "sust_end":   280,
    },
    {
        "file":       "Enel_2024_CSRD.pdf",
        "company":    "Enel",
        "sector":     "Energy",
        "sub_sector": "Utilities (Electricity & Renewables)",
        "country":    "Italy",
        "year":       2024,
        "sust_start": 200,
        "sust_end":   500,
    },
    {
        "file":       "EON_2024_CSRD.pdf",
        "company":    "E.ON",
        "sector":     "Energy",
        "sub_sector": "Utilities (Electricity & Renewables)",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 100,
        "sust_end":   320,
    },
    # --- Telecommunications ---
    {
        "file":       "DeutscheTelekom_2024_CSRD.pdf",
        "company":    "Deutsche Telekom",
        "sector":     "Other",
        "sub_sector": "Telecommunications",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 40,
        "sust_end":   175,
    },
    {
        "file":       "Orange_2024_CSRD.pdf",
        "company":    "Orange",
        "sector":     "Other",
        "sub_sector": "Telecommunications",
        "country":    "France",
        "year":       2024,
        "sust_start": 200,
        "sust_end":   480,
    },
    {
        "file":       "Proximus_2024_CSRD.pdf",
        "company":    "Proximus",
        "sector":     "Other",
        "sub_sector": "Telecommunications",
        "country":    "Belgium",
        "year":       2024,
        "sust_start": 80,
        "sust_end":   260,
    },
    # --- Iron & Steel ---
    {
        "file":       "Thyssenkrupp_2025_CSRD.pdf",
        "company":    "Thyssenkrupp",
        "sector":     "Manufacturing",
        "sub_sector": "Iron & Steel",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 130,
        "sust_end":   280,
    },
    {
        "file":       "Salzgitter_2024_CSRD.pdf",
        "company":    "Salzgitter",
        "sector":     "Manufacturing",
        "sub_sector": "Iron & Steel",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 100,
        "sust_end":   240,
    },
    {
        "file":       "ArcelorMittal_2024_CSRD.pdf",
        "company":    "ArcelorMittal",
        "sector":     "Manufacturing",
        "sub_sector": "Iron & Steel",
        "country":    "Luxembourg",
        "year":       2024,
        "sust_start": 1,
        "sust_end":   200,
    },
    {
        "file":       "SSAB_2024_CSRD.pdf",
        "company":    "SSAB",
        "sector":     "Manufacturing",
        "sub_sector": "Iron & Steel",
        "country":    "Sweden",
        "year":       2024,
        "sust_start": 60,
        "sust_end":   180,
    },
    {
        "file":       "Aperam_2024_CSRD.pdf",
        "company":    "Aperam",
        "sector":     "Manufacturing",
        "sub_sector": "Iron & Steel",
        "country":    "Luxembourg",
        "year":       2024,
        "sust_start": 1,
        "sust_end":   125,
    },
    # --- Food & Beverage ---
    {
        "file":       "Danone_2024_CSRD.pdf",
        "company":    "Danone",
        "sector":     "Manufacturing",
        "sub_sector": "Food & Beverage",
        "country":    "France",
        "year":       2024,
        "sust_start": 1,
        "sust_end":   100,
    },
    {
        "file":       "Nestle_2024_CSRD.pdf",
        "company":    "Nestlé",
        "sector":     "Manufacturing",
        "sub_sector": "Food & Beverage",
        "country":    "Switzerland",
        "year":       2024,
        "sust_start": 1,
        "sust_end":   150,
    },
    # --- Consumer Goods / Apparel ---
    {
        "file":       "Puma_2024_CSRD.pdf",
        "company":    "Puma",
        "sector":     "Manufacturing",
        "sub_sector": "Consumer Goods / Apparel",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 100,
        "sust_end":   290,
    },
    {
        "file":       "HM_Group_2024_CSRD.pdf",
        "company":    "H&M Group",
        "sector":     "Manufacturing",
        "sub_sector": "Consumer Goods / Apparel",
        "country":    "Sweden",
        "year":       2024,
        "sust_start": 1,
        "sust_end":   35,
    },
    {
        "file":       "Inditex_2024_CSRD.pdf",
        "company":    "Inditex",
        "sector":     "Manufacturing",
        "sub_sector": "Consumer Goods / Apparel",
        "country":    "Spain",
        "year":       2024,
        "sust_start": 1,
        "sust_end":   300,
    },
    {
        "file":       "HugoBoss_2024_CSRD.pdf",
        "company":    "Hugo Boss",
        "sector":     "Manufacturing",
        "sub_sector": "Consumer Goods / Apparel",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 62,
        "sust_end":   230,
    },
    # --- Integrated Logistics ---
    {
        "file":       "DSV_2024_CSRD.pdf",
        "company":    "DSV",
        "sector":     "Logistics",
        "sub_sector": "Integrated Logistics & Parcel",
        "country":    "Denmark",
        "year":       2024,
        "sust_start": 50,
        "sust_end":   150,
    },
    {
        "file":       "PostNL_2024_CSRD.pdf",
        "company":    "PostNL",
        "sector":     "Logistics",
        "sub_sector": "Integrated Logistics & Parcel",
        "country":    "Netherlands",
        "year":       2024,
        "sust_start": 50,
        "sust_end":   210,
    },
    # --- Industrial Machinery & Equipment ---
    {
        "file":       "Siemens_2024_CSRD.pdf",
        "company":    "Siemens",
        "sector":     "Manufacturing",
        "sub_sector": "Industrial Machinery & Equipment",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 1,
        "sust_end":   170,
    },
    {
        "file":       "Schneider_Electric_2024_CSRD.pdf",
        "company":    "Schneider Electric",
        "sector":     "Manufacturing",
        "sub_sector": "Industrial Machinery & Equipment",
        "country":    "France",
        "year":       2024,
        "sust_start": 1,
        "sust_end":   200,
    },
    {
        "file":       "Vestas_2024_CSRD.pdf",
        "company":    "Vestas",
        "sector":     "Manufacturing",
        "sub_sector": "Industrial Machinery & Equipment",
        "country":    "Denmark",
        "year":       2024,
        "sust_start": 60,
        "sust_end":   230,
    },
    {
        "file":       "Renault_2024_CSRD.pdf",
        "company":    "Renault Group",
        "sector":     "Manufacturing",
        "sub_sector": "Automotive OEM",
        "country":    "France",
        "year":       2024,
        "sust_start": 200,
        "sust_end":   430,
    },
    # --- Media & Entertainment ---
    {
        "file":       "ProSiebenSat1_2024_CSRD.pdf",
        "company":    "ProSiebenSat.1",
        "sector":     "Other",
        "sub_sector": "Media & Entertainment",
        "country":    "Germany",
        "year":       2024,
        "sust_start": 100,
        "sust_end":   280,
    },
    {
        "file":       "Publicis_2024_CSRD.pdf",
        "company":    "Publicis",
        "sector":     "Other",
        "sub_sector": "Media & Entertainment",
        "country":    "France",
        "year":       2024,
        "sust_start": 147,
        "sust_end":   280,
    },
]

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
  "scope3_by_category": {{                   // fill what is available
    "cat1_purchased_goods": <number or null>,
    "cat11_use_of_sold_products": <number or null>
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
    # Prefer market-based; fall back to location-based then undifferentiated.
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

def build_benchmark_entry(report: dict, kpis: dict, intensities: dict) -> dict:
    """Build a benchmark entry dict for industry_benchmarks.json."""
    return {
        "id": f"csrd-{report['company'].lower().replace(' ', '-')}-{report['year']}",
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
        "document":   report["file"],
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
    parser.add_argument("--dry-run",  action="store_true", help="Print results without writing JSON")
    parser.add_argument("--company",  default=None,        help="Run only this company (substring match)")
    parser.add_argument("--migrate",  action="store_true", help="Apply script rules to existing JSON entries without calling Gemini")
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

    new_entries = []

    for report in reports:
        pdf_path = REPORTS_DIR / report["file"]
        if not pdf_path.exists():
            print(f"\n[skip] {report['file']} not found")
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
