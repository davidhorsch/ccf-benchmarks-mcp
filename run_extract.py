#!/usr/bin/env python3
"""
Wrapper that runs extract_kpis.py using the `claude` CLI for auth
instead of the Anthropic SDK (for session-login users without API key).
"""
import json, subprocess, sys, types, unittest.mock as mock

# --- Minimal stub that mimics anthropic.Anthropic().messages.create() ---

def _claude_cli_create(model, max_tokens, messages, **kw):
    prompt = messages[0]["content"]
    # Pipe prompt via stdin to avoid shell arg-size limits
    result = subprocess.run(
        ["claude", "-p", "--max-turns", "1"],
        input=prompt,
        capture_output=True, text=True, timeout=300
    )
    if result.returncode != 0:
        raise RuntimeError(f"claude CLI error: {result.stderr[:200]}")
    text = result.stdout.strip()
    # Return an object that mimics anthropic Message
    msg = types.SimpleNamespace()
    msg.content = [types.SimpleNamespace(text=text)]
    return msg

class _Messages:
    def create(self, **kw):
        return _claude_cli_create(**kw)

class _FakeAnthropic:
    def __init__(self, **kw):
        self.messages = _Messages()

# Patch the anthropic module before extract_kpis imports it
import anthropic as _real_anthropic
_real_anthropic.Anthropic = _FakeAnthropic

# Now run extract_kpis main
sys.argv = ["extract_kpis.py"] + sys.argv[1:]
import runpy
runpy.run_path("extract_kpis.py", run_name="__main__")
