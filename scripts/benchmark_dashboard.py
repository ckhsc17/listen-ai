#!/usr/bin/env python3
"""Measure latency of POST /api/dashboard (via gateway) for performance reports.

Requires gateway + stat (+ nlp for insert flows; dashboard fast path only needs stat).

Example:
  python scripts/benchmark_dashboard.py --gateway http://localhost:8000 --runs 5
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
import urllib.error
import urllib.request
from datetime import date, timedelta


def http_json(method: str, url: str, payload: dict | None, headers: dict | None, timeout: int) -> tuple[int, dict]:
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method, headers=headers or {})
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            return exc.code, json.loads(raw) if raw else {"error": raw}
        except json.JSONDecodeError:
            return exc.code, {"error": raw}


def login(gateway: str, user: str, password: str) -> str:
    status, body = http_json(
        "POST",
        gateway.rstrip("/") + "/auth/login",
        {"username": user, "password": password},
        None,
        30,
    )
    if status != 200:
        raise RuntimeError(f"login failed: {status} {body}")
    token = body.get("token")
    if not token:
        raise RuntimeError("login response missing token")
    return token


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Benchmark gateway /api/dashboard")
    p.add_argument("--gateway", default="http://localhost:8000")
    p.add_argument("--user", default="admin")
    p.add_argument("--password", default="admin123")
    p.add_argument("--runs", type=int, default=5)
    p.add_argument("--timeout", type=int, default=120)
    p.add_argument(
        "--include-keywords",
        default="機器人",
        help="Comma-separated include keywords (default matches Streamlit demo)",
    )
    p.add_argument("--exclude-keywords", default="")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    gw = args.gateway.rstrip("/")
    include = [k.strip() for k in args.include_keywords.split(",") if k.strip()]
    exclude = [k.strip() for k in args.exclude_keywords.split(",") if k.strip()]

    today = date.today()
    payload = {
        "includeKeywords": include,
        "excludeKeywords": exclude,
        "fromDate": (today - timedelta(days=365 * 10)).strftime("%Y-%m-%d"),
        "toDate": (today + timedelta(days=365)).strftime("%Y-%m-%d"),
        "sampleSize": 5,
    }

    try:
        token = login(gw, args.user, args.password)
    except Exception as exc:  # noqa: BLE001
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    headers = {"Authorization": f"Bearer {token}"}
    times_ms: list[float] = []
    last_body: dict = {}

    for i in range(int(args.runs)):
        t0 = time.perf_counter()
        status, body = http_json("POST", gw + "/api/dashboard", payload, headers, int(args.timeout))
        dt = (time.perf_counter() - t0) * 1000.0
        times_ms.append(dt)
        last_body = body
        if status != 200:
            print(json.dumps({"error": "dashboard failed", "status": status, "body": body}, indent=2))
            return 1

    summary = {
        "gateway": gw,
        "runs": int(args.runs),
        "mean_ms": round(statistics.mean(times_ms), 2),
        "stdev_ms": round(statistics.pstdev(times_ms), 2) if len(times_ms) > 1 else 0.0,
        "min_ms": round(min(times_ms), 2),
        "max_ms": round(max(times_ms), 2),
        "last_mention_count": last_body.get("mentionCount"),
        "last_total_analyzed": last_body.get("totalAnalyzedPosts"),
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
