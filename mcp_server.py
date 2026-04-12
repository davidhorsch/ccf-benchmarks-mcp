#!/usr/bin/env python3
"""
CCF Industry Benchmark MCP Server

Exposes CSRD-derived GHG emission intensity data collected from company
sustainability reports. Use this to benchmark a customer's carbon footprint
against peers in the same sector.

Run locally via Claude Code MCP config (stdio) or deploy to Render (SSE).
No API keys or paid services required for the data itself.
"""

import json
import os
from pathlib import Path
from mcp.server.fastmcp import FastMCP

DATA_FILE = Path(__file__).parent / "industry_benchmarks.json"
with open(DATA_FILE, encoding="utf-8") as f:
    DATA = json.load(f)

mcp = FastMCP(
    "ccf-benchmarks",
    host="0.0.0.0",
    port=int(os.environ.get("PORT", 8000)),
)


@mcp.tool()
def list_companies() -> list[str]:
    """List all companies in the CCF benchmark dataset."""
    return sorted(e["company"] for e in DATA["csrd_company_data"])


@mcp.tool()
def list_sectors() -> dict[str, list[str]]:
    """List all sectors and their sub-sectors available in the benchmark dataset."""
    sectors: dict[str, set] = {}
    for e in DATA["csrd_company_data"]:
        sectors.setdefault(e["sector"], set()).add(e["sub_sector"])
    return {k: sorted(v) for k, v in sorted(sectors.items())}


@mcp.tool()
def get_benchmarks_by_sector(sector: str = "", sub_sector: str = "") -> list[dict]:
    """
    Get GHG emission intensity benchmarks filtered by sector or sub-sector.

    Returns tCO2e/EUR M and tCO2e/FTE intensities for matching companies.
    Partial, case-insensitive matching. Leave both empty to get all companies.

    Key metrics in response:
    - intensity_s12_per_eur_m: Scope 1+2 emissions per EUR million revenue
    - intensity_s123_per_eur_m: Full value chain emissions per EUR million revenue
    - intensity_s12_per_fte: Scope 1+2 emissions per employee
    """
    results = []
    for e in DATA["csrd_company_data"]:
        if sector and sector.lower() not in e["sector"].lower():
            continue
        if sub_sector and sub_sector.lower() not in e["sub_sector"].lower():
            continue
        results.append({
            "company":    e["company"],
            "sector":     e["sector"],
            "sub_sector": e["sub_sector"],
            "country":    e["country"],
            "year":       e["year"],
            "source":     e.get("source_type"),
            "confidence": e.get("confidence"),
            "intensities": e["intensities"],
        })
    results.sort(key=lambda x: x["company"])
    return results


@mcp.tool()
def get_company_benchmark(company: str) -> dict | None:
    """
    Get full benchmark data for a specific company including raw KPIs.
    Partial, case-insensitive matching (e.g. 'basf', 'thyssenkrupp').
    Returns None if not found.
    """
    for e in DATA["csrd_company_data"]:
        if company.lower() in e["company"].lower():
            return e
    return None


@mcp.tool()
def get_eu_ets_benchmarks(product_filter: str = "") -> list[dict]:
    """
    Get EU ETS product benchmarks (tCO2e per tonne of product).

    These are the top-10th-percentile efficiency benchmarks used for EU ETS
    free allowance allocation — best-practice values, not sector averages.
    Average installations typically emit 2–3x these values.

    Optionally filter by product name (partial match).
    """
    values = DATA["eu_ets_product_benchmarks"]["values"]
    if product_filter:
        values = [v for v in values if product_filter.lower() in v["product"].lower()]
    return values


@mcp.tool()
def get_dataset_metadata() -> dict:
    """
    Get metadata about the benchmark dataset: version, caveats, scope
    definitions, and data sources. Read this first to understand limitations.
    """
    return DATA["metadata"]


from starlette.requests import Request
from starlette.responses import JSONResponse


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> JSONResponse:
    """Health check endpoint — no auth required (used by Render)."""
    return JSONResponse({"status": "ok"})


if __name__ == "__main__":
    # Default to streamable-http when PORT is injected by the cloud host (e.g. Render)
    default_transport = "streamable-http" if os.environ.get("PORT") else "stdio"
    transport = os.environ.get("MCP_TRANSPORT", default_transport)

    if transport in ("sse", "streamable-http"):
        import uvicorn
        from starlette.middleware.base import BaseHTTPMiddleware

        class _APIKeyMiddleware(BaseHTTPMiddleware):
            """Block requests without a valid MCP_API_KEY.
            Accepts key via:  X-API-Key header  or  ?api_key= query param.
            /health is always allowed so Render health checks pass.
            """
            async def dispatch(self, request, call_next):
                expected = os.environ.get("MCP_API_KEY", "")
                if expected and request.url.path != "/health":
                    key = (request.headers.get("x-api-key")
                           or request.query_params.get("api_key"))
                    if key != expected:
                        return JSONResponse({"error": "invalid_token", "error_description": "Invalid or missing API key"}, status_code=401)
                return await call_next(request)

        if transport == "streamable-http":
            app = mcp.streamable_http_app()
        else:
            app = mcp.sse_app()
        app.add_middleware(_APIKeyMiddleware)
        uvicorn.run(app, host=mcp.settings.host, port=mcp.settings.port)
    else:
        mcp.run()
