"""Read-only Polymarket market WebSocket canary.

This tool uses the production WS market cache/client code path. It does not
place orders or touch bot state.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from ct_ws_market import DEFAULT_MARKET_WS_URL, WSMarketDataClient


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _load_assets_from_state(path: Path, limit: int) -> list[str]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assets: list[str] = []

    def add(token_id: Any) -> None:
        text = str(token_id or "").strip()
        if text and text not in assets:
            assets.append(text)

    for row in payload.get("my_positions") or []:
        if not isinstance(row, dict):
            continue
        try:
            size = float(row.get("size") or 0)
        except Exception:
            size = 0.0
        if size <= 0:
            continue
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        add(row.get("token_id") or row.get("asset") or raw.get("asset"))
        if len(assets) >= limit:
            break

    if not assets:
        for token_id in payload.get("boot_token_ids") or []:
            add(token_id)
            if len(assets) >= limit:
                break
    return assets[:limit]


def _emit(line: str) -> None:
    print(f"{_now()} {line}", flush=True)


def run(args: argparse.Namespace) -> int:
    assets = [item.strip() for item in (args.asset or []) if item.strip()]
    if args.state:
        assets.extend(_load_assets_from_state(Path(args.state), args.limit))
    assets = list(dict.fromkeys(assets))[: args.limit]
    if not assets:
        raise SystemExit("No asset IDs supplied. Use --asset or --state.")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    proxy_url = ""
    if args.proxy_host and args.proxy_port:
        proxy_url = f"{args.proxy_type}://{args.proxy_host}:{int(args.proxy_port)}"

    client = WSMarketDataClient(
        url=args.url,
        proxy_url=proxy_url,
        ping_interval_sec=args.ping_interval,
        reconnect_backoff_max_sec=args.reconnect_backoff_max,
        max_age_sec=args.max_age,
        logger=logging.getLogger("ws_market_canary"),
    )
    _emit(
        f"START assets={len(assets)} duration={int(args.duration)} "
        f"proxy={proxy_url or 'env/default'}"
    )
    client.start(assets)
    stop_at = time.time() + max(1, int(args.duration))
    next_stats = time.time() + max(5, int(args.stats_interval))
    try:
        while time.time() < stop_at:
            time.sleep(1)
            if time.time() >= next_stats:
                _emit("STATS " + json.dumps(client.stats(), ensure_ascii=False, sort_keys=True))
                next_stats = time.time() + max(5, int(args.stats_interval))
    finally:
        client.stop()

    summary = client.stats()
    summary["duration_sec"] = int(args.duration)
    summary["requested_assets"] = len(assets)
    _emit("SUMMARY " + json.dumps(summary, ensure_ascii=False, sort_keys=True))
    client_counts = summary.get("client") or {}
    counts = summary.get("counts") or {}
    ok = (
        int(client_counts.get("opens") or 0) > 0
        and int(client_counts.get("errors") or 0) == 0
        and int(client_counts.get("pongs") or 0) > 0
        and int(counts.get("message") or 0) > 0
    )
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_MARKET_WS_URL)
    parser.add_argument("--state", help="State JSON to read asset IDs from.")
    parser.add_argument("--asset", action="append", help="Asset/token ID to subscribe to.")
    parser.add_argument("--limit", type=int, default=8)
    parser.add_argument("--duration", type=int, default=75)
    parser.add_argument("--ping-interval", type=int, default=10)
    parser.add_argument("--stats-interval", type=int, default=60)
    parser.add_argument("--max-age", type=float, default=5.0)
    parser.add_argument("--reconnect-backoff-max", type=int, default=60)
    parser.add_argument("--proxy-host", default="")
    parser.add_argument("--proxy-port", type=int, default=0)
    parser.add_argument("--proxy-type", default="http")
    return run(parser.parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
