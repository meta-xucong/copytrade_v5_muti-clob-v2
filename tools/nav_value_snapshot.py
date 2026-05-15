from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT_FOR_IMPORT = Path(__file__).resolve().parents[1]
if str(ROOT_FOR_IMPORT) not in sys.path:
    sys.path.insert(0, str(ROOT_FOR_IMPORT))

from ct_clob_gateway import init_v2_client, preflight_pusd_ready_v2
from ct_data import fetch_positions_norm
from smartmoney_query.poly_martmoney_query.api_client import DataApiClient


def _now_local() -> dt.datetime:
    return dt.datetime.now().astimezone()


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _norm_usdc(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        x = float(value)
    except Exception:
        return None
    # Most v2 payloads expose collateral values in 6-decimal base units.
    if x >= 100000:
        return x / 1_000_000.0
    return x


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _parse_generated_ts(payload: Dict[str, Any]) -> Optional[float]:
    ts = payload.get("generated_ts")
    if ts is not None:
        try:
            out = float(ts)
            if out > 0:
                return out
        except Exception:
            pass
    text = str(payload.get("generated_local") or "").strip()
    if not text:
        return None
    # expected: 2026-05-08 21:28:43 +0800
    for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%d %H:%M:%S"):
        try:
            parsed = dt.datetime.strptime(text, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.datetime.now().astimezone().tzinfo)
            return parsed.timestamp()
        except Exception:
            continue
    return None


def _load_today_net_by_name(logs_dir: Path) -> Dict[str, float]:
    out: Dict[str, float] = {}
    files = sorted(logs_dir.glob("pnl_today_summary_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not files:
        return out
    payload = _read_json(files[0])
    for _addr, item in (payload.get("accounts") or {}).items():
        name = (item or {}).get("name")
        if not name:
            continue
        out[str(name)] = _safe_float((item or {}).get("net"), 0.0)
    return out


def _collect_nav_snapshot(root: Path) -> Dict[str, Any]:
    cfg = _read_json(root / "copytrade_config.json")
    accounts_payload = _read_json(root / "accounts.json")
    accounts = accounts_payload.get("accounts") or []
    logs_dir = root / "logs"

    data_host = cfg.get("data_api_host") or cfg.get("data_api_base") or "https://data-api.polymarket.com"
    size_threshold = _safe_float(cfg.get("size_threshold"), 0.0)
    positions_limit = int(cfg.get("positions_limit") or 500)
    positions_max_pages = int(cfg.get("positions_max_pages") or 20)
    api_timeout_sec = _safe_float(cfg.get("api_timeout_sec"), 20.0)
    force_http = bool(cfg.get("my_positions_force_http", False))

    data_client = DataApiClient(host=data_host)
    today_net_by_name = _load_today_net_by_name(logs_dir)

    rows: List[Dict[str, Any]] = []
    for a in accounts:
        if not bool(a.get("enabled", True)):
            continue
        name = str(a.get("name") or "").strip()
        address = str(a.get("my_address") or "").strip()
        private_key = str(a.get("private_key") or "").strip()
        if not (name and address and private_key):
            continue

        error_parts: List[str] = []
        cash_balance_usdc: Optional[float] = None
        allowance_usdc: Optional[float] = None
        collateral_ready: Optional[bool] = None
        collateral_msg: Optional[str] = None
        positions_count = 0
        position_mark_usdc = 0.0

        try:
            clob_client = init_v2_client(private_key, address, cfg)
            preflight = preflight_pusd_ready_v2(clob_client, api_timeout_sec)
            cash_balance_usdc = _norm_usdc(preflight.get("balance"))
            allowance_usdc = _norm_usdc(preflight.get("allowance"))
            collateral_ready = preflight.get("ready")
            collateral_msg = preflight.get("message")
        except Exception as exc:
            error_parts.append(f"collateral:{exc}")

        try:
            positions, _info = fetch_positions_norm(
                data_client,
                address,
                size_threshold,
                positions_limit=positions_limit,
                positions_max_pages=positions_max_pages,
                force_http=force_http,
                cache_bust_mode="bucket",
            )
            positions_count = len(positions)
            position_mark_usdc = sum(
                _safe_float(p.get("size"), 0.0) * _safe_float(p.get("cur_price"), 0.0) for p in positions
            )
        except Exception as exc:
            error_parts.append(f"positions:{exc}")

        total_value_usdc = _safe_float(cash_balance_usdc, 0.0) + position_mark_usdc
        rows.append(
            {
                "name": name,
                "address": address,
                "cash_balance_usdc": cash_balance_usdc,
                "position_mark_usdc": position_mark_usdc,
                "total_value_usdc": total_value_usdc,
                "positions_count": positions_count,
                "allowance_usdc": allowance_usdc,
                "collateral_ready": collateral_ready,
                "collateral_msg": collateral_msg,
                "today_net_from_latest_snapshot": today_net_by_name.get(name),
                "error": " | ".join(error_parts) if error_parts else None,
            }
        )

    rows.sort(key=lambda r: _safe_float(r.get("total_value_usdc"), 0.0), reverse=True)
    now = _now_local()

    return {
        "generated_local": now.strftime("%Y-%m-%d %H:%M:%S %z"),
        "generated_ts": now.timestamp(),
        "data_host": data_host,
        "cash_unit_note": "USDC; converted from 6-decimal raw units when needed",
        "accounts": rows,
        "totals": {
            "cash_balance_sum_usdc": sum(_safe_float(r.get("cash_balance_usdc"), 0.0) for r in rows),
            "position_mark_sum_usdc": sum(_safe_float(r.get("position_mark_usdc"), 0.0) for r in rows),
            "total_value_sum_usdc": sum(_safe_float(r.get("total_value_usdc"), 0.0) for r in rows),
            "today_net_sum_from_snapshot": sum(
                _safe_float(r.get("today_net_from_latest_snapshot"), 0.0) for r in rows
            ),
        },
    }


def _save_snapshot(root: Path, snapshot: Dict[str, Any]) -> Path:
    logs_dir = root / "logs"
    now = _now_local()
    out_path = logs_dir / f"nav_snapshot_{now.strftime('%Y%m%d_%H%M%S')}.json"
    _write_json(out_path, snapshot)
    _write_json(logs_dir / "nav_snapshot_latest.json", snapshot)
    return out_path


def _find_baseline_snapshot(logs_dir: Path, target_ts: float) -> Tuple[Optional[Path], Optional[Dict[str, Any]], str]:
    files = list(logs_dir.glob("nav_snapshot_*.json"))
    # Backward-compatibility for earlier one-off snapshots.
    files.extend(logs_dir.glob("account_total_value_normalized_*.json"))
    files = sorted(files, key=lambda p: p.stat().st_mtime)
    if not files:
        return None, None, "no_snapshot_files"

    candidates: List[Tuple[Path, Dict[str, Any], float]] = []
    for f in files:
        try:
            payload = _read_json(f)
            ts = _parse_generated_ts(payload)
            if ts is not None and ts > 0:
                payload["_parsed_generated_ts"] = ts
                candidates.append((f, payload, ts))
        except Exception:
            continue
    if not candidates:
        return None, None, "no_valid_snapshot_payload"

    older = [x for x in candidates if x[2] <= target_ts]
    if older:
        best = max(older, key=lambda x: x[2])
        return best[0], best[1], "older_or_equal_to_target"

    best = min(candidates, key=lambda x: abs(x[2] - target_ts))
    return best[0], best[1], "nearest_available_no_older"


def _index_by_name(snapshot: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for row in snapshot.get("accounts") or []:
        name = str((row or {}).get("name") or "").strip()
        if name:
            out[name] = row
    return out


def _build_24h_report(root: Path, current_snapshot_path: Path, current: Dict[str, Any]) -> Tuple[Path, Dict[str, Any]]:
    logs_dir = root / "logs"
    now_ts = float(current.get("generated_ts") or _now_local().timestamp())
    target_ts = now_ts - 24 * 3600

    baseline_path, baseline, pick_mode = _find_baseline_snapshot(logs_dir, target_ts)
    if baseline is None or baseline_path is None:
        report = {
            "generated_local": _now_local().strftime("%Y-%m-%d %H:%M:%S %z"),
            "current_snapshot": str(current_snapshot_path),
            "baseline_snapshot": None,
            "baseline_pick_mode": pick_mode,
            "error": "baseline snapshot not found",
        }
        out_path = logs_dir / f"nav_24h_report_{_now_local().strftime('%Y%m%d_%H%M%S')}.json"
        _write_json(out_path, report)
        return out_path, report

    baseline_ts = _safe_float(baseline.get("_parsed_generated_ts"), 0.0) or _safe_float(
        _parse_generated_ts(baseline), now_ts
    )
    cur_by_name = _index_by_name(current)
    base_by_name = _index_by_name(baseline)
    all_names = sorted(set(cur_by_name) | set(base_by_name))

    rows: List[Dict[str, Any]] = []
    for name in all_names:
        cur = cur_by_name.get(name, {})
        old = base_by_name.get(name, {})
        cur_total = _safe_float(cur.get("total_value_usdc"), 0.0)
        old_total = _safe_float(old.get("total_value_usdc"), 0.0)
        cur_cash = _safe_float(cur.get("cash_balance_usdc"), 0.0)
        old_cash = _safe_float(old.get("cash_balance_usdc"), 0.0)
        cur_pos = _safe_float(cur.get("position_mark_usdc"), 0.0)
        old_pos = _safe_float(old.get("position_mark_usdc"), 0.0)
        rows.append(
            {
                "name": name,
                "current_total_value_usdc": cur_total,
                "baseline_total_value_usdc": old_total,
                "delta_total_value_usdc": cur_total - old_total,
                "current_cash_usdc": cur_cash,
                "baseline_cash_usdc": old_cash,
                "delta_cash_usdc": cur_cash - old_cash,
                "current_position_mark_usdc": cur_pos,
                "baseline_position_mark_usdc": old_pos,
                "delta_position_mark_usdc": cur_pos - old_pos,
            }
        )

    rows.sort(key=lambda r: r["delta_total_value_usdc"])

    cur_tot = current.get("totals") or {}
    old_tot = baseline.get("totals") or {}
    report = {
        "generated_local": _now_local().strftime("%Y-%m-%d %H:%M:%S %z"),
        "current_snapshot": str(current_snapshot_path),
        "baseline_snapshot": str(baseline_path),
        "baseline_pick_mode": pick_mode,
        "window_hours_effective": (now_ts - baseline_ts) / 3600.0,
        "totals": {
            "current_total_value_usdc": _safe_float(cur_tot.get("total_value_sum_usdc"), 0.0),
            "baseline_total_value_usdc": _safe_float(old_tot.get("total_value_sum_usdc"), 0.0),
            "delta_total_value_usdc": _safe_float(cur_tot.get("total_value_sum_usdc"), 0.0)
            - _safe_float(old_tot.get("total_value_sum_usdc"), 0.0),
            "current_cash_sum_usdc": _safe_float(cur_tot.get("cash_balance_sum_usdc"), 0.0),
            "baseline_cash_sum_usdc": _safe_float(old_tot.get("cash_balance_sum_usdc"), 0.0),
            "delta_cash_sum_usdc": _safe_float(cur_tot.get("cash_balance_sum_usdc"), 0.0)
            - _safe_float(old_tot.get("cash_balance_sum_usdc"), 0.0),
            "current_position_mark_sum_usdc": _safe_float(cur_tot.get("position_mark_sum_usdc"), 0.0),
            "baseline_position_mark_sum_usdc": _safe_float(old_tot.get("position_mark_sum_usdc"), 0.0),
            "delta_position_mark_sum_usdc": _safe_float(cur_tot.get("position_mark_sum_usdc"), 0.0)
            - _safe_float(old_tot.get("position_mark_sum_usdc"), 0.0),
        },
        "accounts": rows,
    }

    out_path = logs_dir / f"nav_24h_report_{_now_local().strftime('%Y%m%d_%H%M%S')}.json"
    _write_json(out_path, report)
    _write_json(logs_dir / "nav_24h_report_latest.json", report)
    return out_path, report


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate NAV snapshot and optional 24h report.")
    parser.add_argument("--root", default=".", help="Project root path")
    parser.add_argument("--with-24h-report", action="store_true", help="Also generate a 24h NAV report")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    snapshot = _collect_nav_snapshot(root)
    snapshot_path = _save_snapshot(root, snapshot)

    print(f"SNAPSHOT={snapshot_path}")
    totals = snapshot.get("totals") or {}
    print(
        "TOTAL current_nav="
        f"{_safe_float(totals.get('total_value_sum_usdc'), 0.0):.6f} "
        f"cash={_safe_float(totals.get('cash_balance_sum_usdc'), 0.0):.6f} "
        f"mark={_safe_float(totals.get('position_mark_sum_usdc'), 0.0):.6f}"
    )
    for row in snapshot.get("accounts") or []:
        print(
            f"{row.get('name'):>10} total={_safe_float(row.get('total_value_usdc')):.4f} "
            f"cash={_safe_float(row.get('cash_balance_usdc')):.4f} "
            f"pos={_safe_float(row.get('position_mark_usdc')):.4f}"
        )

    if args.with_24h_report:
        report_path, report = _build_24h_report(root, snapshot_path, snapshot)
        print(f"REPORT24H={report_path}")
        rt = report.get("totals") or {}
        print(
            "TOTAL_24H delta="
            f"{_safe_float(rt.get('delta_total_value_usdc')):.6f} "
            f"(current={_safe_float(rt.get('current_total_value_usdc')):.6f}, "
            f"baseline={_safe_float(rt.get('baseline_total_value_usdc')):.6f}) "
            f"effective_hours={_safe_float(report.get('window_hours_effective')):.3f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
