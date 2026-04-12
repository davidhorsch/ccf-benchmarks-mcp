#!/usr/bin/env python3
"""
CCF Industry Benchmark MCP Server

Exposes CSRD-derived GHG emission intensity data collected from company
sustainability reports. Use this to benchmark a customer's carbon footprint
against peers in the same sector.

Run locally via Claude Code MCP config (stdio) or deploy to Render (SSE).

Auth (cloud only):
  Set OAUTH_CLIENT_SECRET in Render env vars to enable OAuth 2.0 Client
  Credentials. Leave unset to run without auth (fine for public data).
  When enabled, clients obtain a Bearer token via POST /token and pass it
  as Authorization: Bearer <token>. claude.ai connectors do this automatically
  when you enter the Client ID / Client Secret in the connector settings.
"""

import json
import os
import secrets
import time
from pathlib import Path

from mcp.server.fastmcp import FastMCP
from starlette.requests import Request
from starlette.responses import JSONResponse

DATA_FILE = Path(__file__).parent / "industry_benchmarks.json"
with open(DATA_FILE, encoding="utf-8") as f:
    DATA = json.load(f)

BASE_URL = os.environ.get("BASE_URL", "https://csrd-benchmarks-mcp.onrender.com")
TOKEN_TTL = 3600  # seconds

mcp = FastMCP(
    "ccf-benchmarks",
    host="0.0.0.0",
    port=int(os.environ.get("PORT", 8000)),
)


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# OAuth 2.0 Client Credentials endpoints
# ---------------------------------------------------------------------------

@mcp.custom_route("/.well-known/oauth-protected-resource", methods=["GET"])
async def oauth_protected_resource(request: Request) -> JSONResponse:
    """RFC 9728 — tells clients which auth server protects this resource."""
    return JSONResponse({
        "resource": BASE_URL,
        "authorization_servers": [BASE_URL],
    })


@mcp.custom_route("/.well-known/oauth-authorization-server", methods=["GET"])
async def oauth_authorization_server(request: Request) -> JSONResponse:
    """RFC 8414 — auth server metadata discovery."""
    return JSONResponse({
        "issuer": BASE_URL,
        "token_endpoint": f"{BASE_URL}/token",
        "grant_types_supported": ["client_credentials"],
        "token_endpoint_auth_methods_supported": ["client_secret_post"],
        "response_types_supported": ["token"],
    })


@mcp.custom_route("/token", methods=["POST"])
async def token_endpoint(request: Request) -> JSONResponse:
    """
    OAuth 2.0 token endpoint — client_credentials grant.

    Expects application/x-www-form-urlencoded body:
      grant_type=client_credentials&client_id=<id>&client_secret=<secret>

    Returns a signed Bearer token valid for TOKEN_TTL seconds.
    If OAUTH_CLIENT_SECRET is not set, issues tokens freely (open server).
    """
    client_secret_env = os.environ.get("OAUTH_CLIENT_SECRET", "")

    if client_secret_env:
        form = await request.form()
        grant_type = form.get("grant_type", "")
        client_id = form.get("client_id", "")
        client_secret = form.get("client_secret", "")

        if grant_type != "client_credentials":
            return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)

        expected_id = os.environ.get("OAUTH_CLIENT_ID", "ccf-benchmarks")
        if client_id != expected_id or not secrets.compare_digest(client_secret, client_secret_env):
            return JSONResponse({"error": "invalid_client"}, status_code=401)

    # Issue a simple signed token: header.payload.sig (no PyJWT dependency)
    now = int(time.time())
    token = _sign_token({"exp": now + TOKEN_TTL, "iat": now})

    return JSONResponse({
        "access_token": token,
        "token_type": "bearer",
        "expires_in": TOKEN_TTL,
    })


@mcp.custom_route("/health", methods=["GET"])
async def health_check(request: Request) -> JSONResponse:
    """Health check — no auth required (used by Render)."""
    return JSONResponse({"status": "ok"})


# ---------------------------------------------------------------------------
# Minimal HMAC token helpers (no extra dependencies)
# ---------------------------------------------------------------------------

import hashlib
import hmac
import base64


def _token_secret() -> bytes:
    """Signing key — stable across requests, rotates on restart (fine for TTL tokens)."""
    raw = os.environ.get("OAUTH_CLIENT_SECRET") or os.environ.get("JWT_SECRET") or "dev-only-insecure"
    return raw.encode()


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _sign_token(payload: dict) -> str:
    body = _b64(json.dumps(payload, separators=(",", ":")).encode())
    sig = _b64(hmac.new(_token_secret(), body.encode(), hashlib.sha256).digest())
    return f"{body}.{sig}"


def _verify_token(token: str) -> bool:
    try:
        body, sig = token.rsplit(".", 1)
        expected_sig = _b64(hmac.new(_token_secret(), body.encode(), hashlib.sha256).digest())
        if not secrets.compare_digest(sig, expected_sig):
            return False
        payload = json.loads(base64.urlsafe_b64decode(body + "=="))
        return int(time.time()) < payload["exp"]
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Default to streamable-http when PORT is injected by the cloud host (e.g. Render)
    default_transport = "streamable-http" if os.environ.get("PORT") else "stdio"
    transport = os.environ.get("MCP_TRANSPORT", default_transport)

    if transport in ("sse", "streamable-http"):
        import uvicorn
        from starlette.middleware.base import BaseHTTPMiddleware
        from starlette.middleware.cors import CORSMiddleware

        # Paths that must be reachable without a Bearer token
        OPEN_PATHS = {
            "/health",
            "/token",
            "/.well-known/oauth-authorization-server",
            "/.well-known/oauth-protected-resource",
        }

        class _BearerAuthMiddleware(BaseHTTPMiddleware):
            async def dispatch(self, request, call_next):
                # Skip auth if no secret configured or path is open
                if not os.environ.get("OAUTH_CLIENT_SECRET") or request.url.path in OPEN_PATHS:
                    return await call_next(request)

                # OPTIONS preflights must pass through (CORS middleware is outermost)
                if request.method == "OPTIONS":
                    return await call_next(request)

                auth = request.headers.get("authorization", "")
                if auth.startswith("Bearer ") and _verify_token(auth[7:]):
                    return await call_next(request)

                return JSONResponse(
                    {"error": "invalid_token", "error_description": "Bearer token required"},
                    status_code=401,
                    headers={
                        "WWW-Authenticate": (
                            f'Bearer realm="ccf-benchmarks",'
                            f' resource_metadata="{BASE_URL}/.well-known/oauth-protected-resource"'
                        )
                    },
                )

        if transport == "streamable-http":
            app = mcp.streamable_http_app()
        else:
            app = mcp.sse_app()

        # Starlette middleware is applied in reverse add order (last added = outermost).
        # Order here: _BearerAuthMiddleware added first → CORSMiddleware added last = outermost.
        # This ensures OPTIONS preflights are handled by CORS before auth runs.
        app.add_middleware(_BearerAuthMiddleware)
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
            allow_headers=["*"],
            expose_headers=["*"],
        )
        uvicorn.run(app, host=mcp.settings.host, port=mcp.settings.port)
    else:
        mcp.run()
