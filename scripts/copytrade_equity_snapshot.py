#!/usr/bin/env python3
from __future__ import annotations

import csv
import io
import json
import sys
import time
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
APP_DIR = ROOT / "netlify" / "copytrade-equity-dashboard"
ACCOUNTS_PATH = APP_DIR / "data" / "accounts.json"
HISTORY_PATH = APP_DIR / "data" / "equity-history.json"
DATA_API = "https://data-api.polymarket.com"
USER_AGENT = "Mozilla/5.0"
MAX_HISTORY_POINTS = 24 * 120
REQUEST_RETRIES = 4
ACCOUNT_SLEEP_SEC = 0.4


def load_json(path: Path, fallback: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return fallback


def request_bytes(path: str, params: dict[str, Any], timeout: float = 60.0) -> bytes:
    query = urllib.parse.urlencode(params)
    req = urllib.request.Request(
        f"{DATA_API}{path}?{query}",
        headers={"User-Agent": USER_AGENT},
    )
    last_error: Exception | None = None
    for attempt in range(REQUEST_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except Exception as exc:
            last_error = exc
            if attempt == REQUEST_RETRIES - 1:
                break
            time.sleep(1.2 * (attempt + 1))
    raise last_error or RuntimeError(f"request failed: {path}")


def request_json(path: str, params: dict[str, Any]) -> Any:
    return json.loads(request_bytes(path, params).decode("utf-8"))


def fetch_positions_value(safe: str) -> tuple[float, float, int]:
    positions: list[dict[str, Any]] = []
    offset = 0
    while True:
        batch = request_json(
            "/positions",
            {"user": safe, "limit": 500, "offset": offset, "sizeThreshold": 0},
        )
        if not isinstance(batch, list) or not batch:
            break
        positions.extend(batch)
        if len(batch) < 500:
            break
        offset += len(batch)

    open_value = sum(float(item.get("currentValue") or 0) for item in positions)
    redeemable_value = sum(
        float(item.get("currentValue") or 0)
        for item in positions
        if item.get("redeemable") is True
    )
    return open_value, redeemable_value, len(positions)


def fetch_accounting_snapshot(safe: str) -> dict[str, Any]:
    payload = request_bytes("/v1/accounting/snapshot", {"user": safe})
    with zipfile.ZipFile(io.BytesIO(payload)) as zf:
        rows = list(csv.DictReader(io.StringIO(zf.read("equity.csv").decode("utf-8"))))
    if not rows:
        raise RuntimeError(f"empty equity.csv for {safe}")
    row = rows[0]
    open_value, redeemable_value, positions_count = fetch_positions_value(safe)
    return {
        "cash": float(row.get("cashBalance") or 0),
        "positionsValue": float(row.get("positionsValue") or 0),
        "equity": float(row.get("equity") or 0),
        "valuationTime": row.get("valuationTime") or None,
        "openValue": open_value,
        "redeemableValue": redeemable_value,
        "positionsCount": positions_count,
    }


def round6(value: float) -> float:
    return round(float(value), 6)


def build_snapshot(accounts: list[dict[str, Any]]) -> dict[str, Any]:
    captured_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    rows = []
    failures = []

    for account in accounts:
        label = str(account["label"])
        safe = str(account["safe"])
        principal = float(account.get("principal") or 0)
        try:
            raw = fetch_accounting_snapshot(safe)
            equity = float(raw["equity"])
            rows.append(
                {
                    "label": label,
                    "safe": safe,
                    "principal": round6(principal),
                    "equity": round6(equity),
                    "pnl": round6(equity - principal),
                    "cash": round6(raw["cash"]),
                    "openValue": round6(raw["openValue"]),
                    "redeemableValue": round6(raw["redeemableValue"]),
                    "positionsCount": int(raw["positionsCount"]),
                    "valuationTime": raw["valuationTime"],
                }
            )
        except Exception as exc:
            failures.append({"label": label, "safe": safe, "error": str(exc)})
        time.sleep(ACCOUNT_SLEEP_SEC)

    total_principal = sum(row["principal"] for row in rows)
    total_equity = sum(row["equity"] for row in rows)
    total_pnl = total_equity - total_principal
    return {
        "capturedAt": captured_at,
        "accounts": rows,
        "summary": {
            "accountCount": len(rows),
            "totalPrincipal": round6(total_principal),
            "totalEquity": round6(total_equity),
            "totalPnl": round6(total_pnl),
            "totalPnlPct": round6((total_pnl / total_principal) * 100) if total_principal else 0,
        },
        "failures": failures,
    }


def merge_history(history_doc: dict[str, Any], snapshot: dict[str, Any]) -> dict[str, Any]:
    history = list(history_doc.get("history") or [])
    history = [item for item in history if item.get("capturedAt") != snapshot["capturedAt"]]
    history.append(snapshot)
    history.sort(key=lambda item: str(item.get("capturedAt") or ""))
    if len(history) > MAX_HISTORY_POINTS:
        history = history[-MAX_HISTORY_POINTS:]
    return {
        "schemaVersion": 1,
        "source": f"{DATA_API}/v1/accounting/snapshot",
        "updatedAt": snapshot["capturedAt"],
        "latest": snapshot,
        "history": history,
    }


def main() -> int:
    accounts_doc = load_json(ACCOUNTS_PATH, {})
    accounts = accounts_doc.get("accounts")
    if not isinstance(accounts, list) or not accounts:
        raise SystemExit(f"missing accounts in {ACCOUNTS_PATH}")

    snapshot = build_snapshot(accounts)
    if len(snapshot["accounts"]) == 0:
        raise SystemExit(f"all account snapshots failed: {snapshot['failures']}")

    history_doc = load_json(HISTORY_PATH, {})
    merged = merge_history(history_doc, snapshot)
    HISTORY_PATH.write_text(json.dumps(merged, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        json.dumps(
            {
                "capturedAt": snapshot["capturedAt"],
                "accountCount": snapshot["summary"]["accountCount"],
                "totalEquity": snapshot["summary"]["totalEquity"],
                "totalPnl": snapshot["summary"]["totalPnl"],
                "failures": snapshot["failures"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
