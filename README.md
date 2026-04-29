# CCF Industry Benchmark MCP Server

GHG emission intensity benchmarks extracted from EU company CSRD reports, exposed as an MCP (Model Context Protocol) server. Lets AI assistants like Claude answer questions like *"How does our customer's carbon footprint compare to peers in the chemical sector?"*

## What's in the dataset

- **55 companies** across Manufacturing, Logistics, Energy, and Other sectors
- **12 European countries** — FY2023 and FY2024 data
- **Source**: First-year CSRD sustainability disclosures (company-published PDFs)
- **5 EU ETS product benchmarks** (top-10th-percentile efficiency standards)

Sectors and sub-sectors covered:

| Sector | Sub-sectors |
| --- | --- |
| Manufacturing | Chemical Industry, Automotive OEM, Iron & Steel, Food & Beverage, Industrial Machinery & Equipment, Consumer Goods / Apparel |
| Energy | Oil & Gas, Utilities (Electricity & Renewables) |
| Logistics | Integrated Logistics & Parcel |
| Other | Telecommunications, Media & Entertainment, Financial Services |

Key metrics per company:
| Metric | Description |
| --- | --- |
| `intensity_s12_per_eur_m` | Scope 1+2 tCO2e per EUR million revenue |
| `intensity_s123_per_eur_m` | Full value chain tCO2e per EUR million revenue |
| `intensity_s12_per_fte` | Scope 1+2 tCO2e per employee |

---

## Repository structure

```
ccf-benchmarks-mcp/
├── mcp_server.py               MCP server (serves the benchmarks via stdio or SSE)
├── industry_benchmarks.json    The dataset — output of the extraction pipeline
├── benchmarks_visualization.html  Quick local viz of the data
│
├── extract_kpis.py             Pipeline: extracts KPIs from PDFs via Gemini 2.5 Flash
├── srnav_downloader.py         Pipeline: downloads CSRD PDFs from srnav.com
├── run_extract.py              Pipeline: CLI wrapper — use instead of extract_kpis.py
│                               when authenticated via `claude` CLI rather than API key
│
├── requirements.txt            Server dependencies (mcp, uvicorn)
├── pipeline-requirements.txt   Pipeline dependencies (google-genai, requests, etc.)
└── render.yaml                 Render.com deploy config
```

> `csrd_reports/` (the raw PDFs + `metadata.csv`, ~600MB of PDFs) is gitignored. Download locally with `srnav_downloader.py`.

---

## Using the MCP server

### Hosted (Render) — recommended

The server runs publicly at:
```
https://csrd-benchmarks-mcp.onrender.com/mcp
```

Connect in Claude Code:
```bash
claude mcp add ccf-benchmarks --url "https://csrd-benchmarks-mcp.onrender.com/mcp"
```

Or add the entry manually to `~/.claude.json` / `claude_desktop_config.json`:
```json
{
  "mcpServers": {
    "ccf-benchmarks": {
      "type": "http",
      "url": "https://csrd-benchmarks-mcp.onrender.com/mcp",
      "headers": {
        "Authorization": "Bearer YOUR_TOKEN_HERE"
      }
    }
  }
}
```

Or add as a custom connector on **claude.ai web** (Settings → Customize → Connectors → + → Add custom connector):
```
https://csrd-benchmarks-mcp.onrender.com/mcp
```
Once added on web, it syncs to the mobile app automatically.

> Free Render tier spins down after 15 min idle — first request after that takes ~30s.

### Local (stdio)

```bash
pip install -r requirements.txt
python mcp_server.py
```

Add to `~/.claude.json` under `mcpServers`:
```json
"ccf-benchmarks": {
  "type": "stdio",
  "command": "python3",
  "args": ["/path/to/mcp_server.py"]
}
```

### Available tools

| Tool | Description |
| --- | --- |
| `list_companies()` | All 55 companies in the dataset |
| `list_sectors()` | Sectors and sub-sectors |
| `get_benchmarks_by_sector(sector, sub_sector)` | Intensity benchmarks filtered by sector — partial, case-insensitive |
| `get_raw_kpis(sector, sub_sector, company)` | Absolute Scope 1/2/3 figures, revenue, and FTE — not intensity ratios |
| `get_company_benchmark(company)` | Full data for one company including raw KPIs — partial match |
| `get_eu_ets_benchmarks(product_filter)` | EU ETS product benchmarks (tCO2e per tonne of product) |
| `get_dataset_metadata()` | Version, caveats, scope definitions — read this first |

---

## Updating the dataset

The pipeline has two stages: download → extract. Each stage is idempotent — re-running only processes what isn't already present.

### 1. Download new CSRD reports

```bash
pip install -r pipeline-requirements.txt
python srnav_downloader.py
```

Downloads PDFs into `csrd_reports/` and appends new entries to `csrd_reports/metadata.csv`. Companies already in `metadata.csv` are skipped automatically.

```bash
python srnav_downloader.py --max 10 --dry-run   # preview selection without downloading
python srnav_downloader.py --max 20              # download up to 20 new companies
```

`metadata.csv` is the source of truth for the extraction pipeline. Each row maps a company to a local PDF, its page range for the sustainability section, and srnav.com metadata.

### 2. Extract KPIs with Gemini

Requires a `GOOGLE_API_KEY` in `.env`:
```bash
echo "GOOGLE_API_KEY=your_key_here" > .env
```

```bash
python extract_kpis.py                           # process all new reports (skips already extracted)
python extract_kpis.py --company BASF            # single company (skips if already extracted)
python extract_kpis.py --company BASF --force    # re-extract and overwrite one company
python extract_kpis.py --force                   # re-extract everything
python extract_kpis.py --dry-run                 # print results without writing JSON
python extract_kpis.py --migrate --dry-run       # preview rule-based fixes to existing JSON entries
```

Gemini 2.5 Flash reads each PDF directly (no text pre-processing) and extracts:
- Scope 1, 2 (location-based + market-based), 3 emissions
- Revenue (EUR million)
- FTE headcount

Results are upserted into `industry_benchmarks.json`. Each run only calls Gemini for companies not yet present in the JSON — use `--force` to re-extract.

> If you're authenticated via the `claude` CLI rather than an API key, use `python run_extract.py` instead of `extract_kpis.py` — it wraps the same pipeline using the CLI session for auth.

### 3. Deploy updated data

```bash
git add industry_benchmarks.json
git commit -m "Update benchmarks: <description>"
git push
```

Render auto-deploys on push.

---

## Auth

The hosted server is protected by an API key. Pass it as:
- Header: `Authorization: Bearer KEY`
- Header: `X-API-Key: KEY`
- Query param: `?api_key=KEY`

The `/health` endpoint is always public (used by Render's health check).

To rotate the key: update `MCP_API_KEY` in Render's Environment settings.
