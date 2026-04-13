# CCF Industry Benchmark MCP Server

GHG emission intensity benchmarks extracted from EU company CSRD reports, exposed as an MCP (Model Context Protocol) server. Lets AI assistants like Claude answer questions like *"How does our customer's carbon footprint compare to peers in the chemical sector?"*

## What's in the dataset

- **52 companies** across Manufacturing, Logistics, Energy, and other sectors
- **11 European countries** — FY2023 and FY2024 data
- **Source**: First-year CSRD sustainability disclosures (company-published PDFs)
- **5 EU ETS product benchmarks** (top-10th-percentile efficiency standards)

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
├── run_extract.py              Pipeline: CLI wrapper for extract_kpis
│
├── requirements.txt            Server dependencies
├── pipeline-requirements.txt   Pipeline dependencies
└── render.yaml                 Render.com deploy config
```

> `csrd_reports/` (the raw PDFs, ~586MB) is gitignored. Download locally with `srnav_downloader.py`.

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

#Attention: add the header to the config file:
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

Or in `claude_desktop_config.json`:
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
| `list_companies()` | All 52 companies in the dataset |
| `list_sectors()` | Sectors and sub-sectors |
| `get_benchmarks_by_sector(sector, sub_sector)` | Filter by sector — partial, case-insensitive |
| `get_company_benchmark(company)` | Full data for one company — partial match |
| `get_eu_ets_benchmarks(product_filter)` | EU ETS product benchmarks |
| `get_dataset_metadata()` | Version, caveats, scope definitions |

---

## Updating the dataset

### 1. Download new CSRD reports

```bash
pip install -r pipeline-requirements.txt
python srnav_downloader.py
```

PDFs land in `csrd_reports/`. Options:
```bash
python srnav_downloader.py --max 10 --dry-run   # preview only
python srnav_downloader.py --output /custom/path
```

### 2. Extract KPIs with Gemini

Requires a `GOOGLE_API_KEY` in `.env`:
```bash
echo "GOOGLE_API_KEY=your_key_here" > .env

python extract_kpis.py                        # process all reports
python extract_kpis.py --company BASF         # single company
python extract_kpis.py --dry-run              # print results, don't write JSON
```

Gemini 2.5 Flash reads each PDF directly (no text pre-processing) and extracts:
- Scope 1, 2 (location-based + market-based), 3 emissions
- Revenue (EUR million)
- FTE headcount

Results are written directly into `industry_benchmarks.json`.

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
- Query param: `?api_key=KEY`
- Header: `X-API-Key: KEY`

The `/health` endpoint is always public (used by Render's health check).

To rotate the key: update `MCP_API_KEY` in Render's Environment settings.
