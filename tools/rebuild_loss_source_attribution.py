from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _norm_source(value: Any) -> str:
    return str(value or "").strip().lower()


def _shorten_source(addr: str) -> str:
    text = _norm_source(addr)
    if text.startswith("0x") and len(text) >= 10:
        return f"{text[:6]}..{text[-4:]}"
    return text


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _load_json(path: Path) -> Dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"JSON root must be object: {path}")
    return payload


def _iter_skip_log_files(logs_dir: Path) -> List[Path]:
    patterns = [
        "copytrade-node-*/target_level_skip_log.jsonl",
        "instances/copytrade-node-*/logs/target_level_skip_log.jsonl",
    ]
    files: List[Path] = []
    seen: set[Path] = set()
    for pat in patterns:
        for p in sorted(logs_dir.glob(pat)):
            rp = p.resolve()
            if rp in seen:
                continue
            seen.add(rp)
            files.append(p)
    return files


def _iter_state_files(logs_dir: Path) -> List[Path]:
    return sorted(logs_dir.glob("instances/copytrade-node-*/logs/state/state_*.json"))


def _iter_source_vote_log_files(logs_dir: Path) -> List[Path]:
    patterns = [
        "copytrade-node-*/token_source_vote_log.jsonl",
        "instances/copytrade-node-*/logs/token_source_vote_log.jsonl",
    ]
    files: List[Path] = []
    seen: set[Path] = set()
    for pat in patterns:
        for p in sorted(logs_dir.glob(pat)):
            rp = p.resolve()
            if rp in seen:
                continue
            seen.add(rp)
            files.append(p)
    return files


def _acc_vote(votes: Dict[str, Dict[str, int]], token_id: str, source: str, weight: int) -> None:
    tid = str(token_id or "").strip()
    src = _norm_source(source)
    if not tid or not src or weight <= 0:
        return
    bucket = votes.setdefault(tid, {})
    bucket[src] = int(bucket.get(src) or 0) + int(weight)


def _build_skip_log_votes(logs_dir: Path) -> Tuple[Dict[str, Dict[str, int]], Dict[str, Any]]:
    votes: Dict[str, Dict[str, int]] = {}
    files = _iter_skip_log_files(logs_dir)
    parsed_lines = 0
    bad_lines = 0
    for path in files:
        for line in path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text:
                continue
            try:
                item = json.loads(text)
            except Exception:
                bad_lines += 1
                continue
            parsed_lines += 1
            if not isinstance(item, dict):
                continue
            _acc_vote(
                votes,
                token_id=str(item.get("token_id") or "").strip(),
                source=str(item.get("source_target") or "").strip(),
                weight=1,
            )
    meta = {
        "skip_log_files": len(files),
        "skip_log_lines_parsed": parsed_lines,
        "skip_log_lines_bad": bad_lines,
        "skip_log_tokens": len(votes),
    }
    return votes, meta


def _build_state_votes(logs_dir: Path) -> Tuple[Dict[str, Dict[str, int]], Dict[str, Any]]:
    votes: Dict[str, Dict[str, int]] = {}
    files = _iter_state_files(logs_dir)
    valid_states = 0
    for path in files:
        try:
            state = _load_json(path)
        except Exception:
            continue
        valid_states += 1
        hist = state.get("token_source_history")
        if isinstance(hist, dict):
            for token_id, entry in hist.items():
                if not isinstance(entry, dict):
                    continue
                by_source = entry.get("by_source")
                if isinstance(by_source, dict) and by_source:
                    for source, count in by_source.items():
                        _acc_vote(votes, str(token_id), source, _safe_int(count))
                else:
                    _acc_vote(
                        votes,
                        str(token_id),
                        entry.get("primary_source"),
                        1,
                    )
        topic_state = state.get("topic_state")
        if isinstance(topic_state, dict):
            for token_id, entry in topic_state.items():
                if not isinstance(entry, dict):
                    continue
                _acc_vote(
                    votes,
                    str(token_id),
                    entry.get("primary_entry_source"),
                    1,
                )
    meta = {
        "state_files": len(files),
        "state_files_loaded": valid_states,
        "state_tokens": len(votes),
    }
    return votes, meta


def _build_source_vote_log_votes(logs_dir: Path) -> Tuple[Dict[str, Dict[str, int]], Dict[str, Any]]:
    votes: Dict[str, Dict[str, int]] = {}
    files = _iter_source_vote_log_files(logs_dir)
    parsed_lines = 0
    bad_lines = 0
    for path in files:
        for line in path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text:
                continue
            try:
                item = json.loads(text)
            except Exception:
                bad_lines += 1
                continue
            parsed_lines += 1
            if not isinstance(item, dict):
                continue
            token_id = str(item.get("token_id") or "").strip()
            source = str(item.get("source_target") or "").strip()
            weight = _safe_int(item.get("source_votes"))
            if weight <= 0:
                weight = 1
            _acc_vote(votes, token_id=token_id, source=source, weight=weight)
    meta = {
        "source_vote_log_files": len(files),
        "source_vote_log_lines_parsed": parsed_lines,
        "source_vote_log_lines_bad": bad_lines,
        "source_vote_log_tokens": len(votes),
    }
    return votes, meta


def _pick_best_source(vote_map: Dict[str, int]) -> Tuple[str, int, int]:
    if not vote_map:
        return "", 0, 0
    total = sum(max(0, _safe_int(v)) for v in vote_map.values())
    src, cnt = max(
        vote_map.items(),
        key=lambda kv: (_safe_int(kv[1]), str(kv[0])),
    )
    return _norm_source(src), _safe_int(cnt), _safe_int(total)


def _group_top_loss_sources(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        source = _norm_source(row.get("source") or "unknown") or "unknown"
        g = grouped.setdefault(
            source,
            {
                "source": source,
                "source_short": _shorten_source(source),
                "loss_sum": 0.0,
                "open_loss_sum": 0.0,
                "token_count": 0,
                "open_token_count": 0,
                "top_tokens": [],
            },
        )
        pnl_like = _safe_float(row.get("pnl_like"))
        current_value = _safe_float(row.get("current_value"))
        g["loss_sum"] += pnl_like
        g["token_count"] += 1
        if current_value > 0:
            g["open_loss_sum"] += pnl_like
            g["open_token_count"] += 1
        g["top_tokens"].append(
            {
                "token_id": str(row.get("token_id") or ""),
                "title": str(row.get("title") or ""),
                "outcome": str(row.get("outcome") or ""),
                "pnl_like": pnl_like,
                "current_value": current_value,
                "accounts": _safe_int(row.get("accounts")),
            }
        )
    out = list(grouped.values())
    for item in out:
        item["top_tokens"] = sorted(
            item["top_tokens"],
            key=lambda tok: _safe_float(tok.get("pnl_like")),
        )[:8]
    out.sort(key=lambda item: _safe_float(item.get("loss_sum")))
    return out


def _merge_source_log_votes(existing: Any, source: str, votes: int) -> Dict[str, int]:
    merged: Dict[str, int] = {}
    if isinstance(existing, dict):
        for key, val in existing.items():
            merged[str(key)] = _safe_int(val)
    short_key = _shorten_source(source)
    merged[short_key] = max(_safe_int(merged.get(short_key)), _safe_int(votes))
    return merged


def _backfill_top_losers(
    rows: Iterable[Dict[str, Any]],
    skip_votes: Dict[str, Dict[str, int]],
    source_vote_log_votes: Dict[str, Dict[str, int]],
    state_votes: Dict[str, Dict[str, int]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    out_rows: List[Dict[str, Any]] = []
    before_unknown_count = 0
    before_unknown_pnl = 0.0
    after_unknown_count = 0
    after_unknown_pnl = 0.0
    mapped_count = 0
    mapped_pnl = 0.0

    for row in rows:
        item = dict(row)
        pnl_like = _safe_float(item.get("pnl_like"))
        source_raw = _norm_source(item.get("source"))
        token_id = str(item.get("token_id") or "").strip()
        if source_raw:
            out_rows.append(item)
            continue

        before_unknown_count += 1
        before_unknown_pnl += pnl_like

        chosen_source = ""
        chosen_votes = 0
        chosen_total = 0
        method = ""

        skip_map = skip_votes.get(token_id, {})
        chosen_source, chosen_votes, chosen_total = _pick_best_source(skip_map)
        if chosen_source:
            method = "skip_log"
        else:
            vote_log_map = source_vote_log_votes.get(token_id, {})
            chosen_source, chosen_votes, chosen_total = _pick_best_source(vote_log_map)
            if chosen_source:
                method = "source_vote_log"
            else:
                state_map = state_votes.get(token_id, {})
                chosen_source, chosen_votes, chosen_total = _pick_best_source(state_map)
                if chosen_source:
                    method = "state_history"

        if chosen_source:
            mapped_count += 1
            mapped_pnl += pnl_like
            item["source"] = chosen_source
            item["source_short"] = _shorten_source(chosen_source)
            item["source_backfill_method"] = method
            item["source_backfill_votes"] = {
                "winner_votes": chosen_votes,
                "total_votes": chosen_total,
            }
            item["source_log_votes"] = _merge_source_log_votes(
                item.get("source_log_votes"),
                chosen_source,
                chosen_votes,
            )
        else:
            after_unknown_count += 1
            after_unknown_pnl += pnl_like

        out_rows.append(item)

    summary = {
        "unknown_before_count": before_unknown_count,
        "unknown_before_pnl_sum": before_unknown_pnl,
        "unknown_after_count": after_unknown_count,
        "unknown_after_pnl_sum": after_unknown_pnl,
        "mapped_count": mapped_count,
        "mapped_pnl_sum": mapped_pnl,
        "mapped_ratio_by_count": (mapped_count / before_unknown_count) if before_unknown_count > 0 else 0.0,
        "mapped_ratio_by_pnl": (mapped_pnl / before_unknown_pnl) if before_unknown_pnl != 0 else 0.0,
    }
    return out_rows, summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill loss source attribution using skip logs/state.")
    parser.add_argument(
        "--input",
        default="logs/analysis_current_loss_sources_latest.json",
        help="Input loss-source snapshot JSON.",
    )
    parser.add_argument(
        "--logs-dir",
        default="logs",
        help="Logs directory containing skip logs/state files.",
    )
    parser.add_argument(
        "--output-prefix",
        default="analysis_current_loss_sources_backfilled",
        help="Output filename prefix under logs directory.",
    )
    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parents[1]
    input_path = (base_dir / args.input).resolve()
    logs_dir = (base_dir / args.logs_dir).resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")
    if not logs_dir.exists():
        raise FileNotFoundError(f"Logs directory not found: {logs_dir}")

    payload = _load_json(input_path)
    top_losers = payload.get("top_losers")
    if not isinstance(top_losers, list):
        raise ValueError("input top_losers must be a list")

    skip_votes, skip_meta = _build_skip_log_votes(logs_dir)
    source_vote_log_votes, source_vote_log_meta = _build_source_vote_log_votes(logs_dir)
    state_votes, state_meta = _build_state_votes(logs_dir)
    updated_rows, backfill_summary = _backfill_top_losers(
        top_losers,
        skip_votes,
        source_vote_log_votes,
        state_votes,
    )
    top_loss_sources = _group_top_loss_sources(updated_rows)

    generated_at = _now_iso()
    main_out = {
        "generated_at": generated_at,
        "input_file": str(input_path),
        "source_map_info": {
            "method": "skip_log_then_source_vote_log_then_state_history",
            "skip_meta": skip_meta,
            "source_vote_log_meta": source_vote_log_meta,
            "state_meta": state_meta,
        },
        "backfill_summary": backfill_summary,
        "top_losers": updated_rows,
    }
    by_source_out = {
        "generated_at": generated_at,
        "source_map_info": main_out["source_map_info"],
        "backfill_summary": backfill_summary,
        "top_loss_sources": top_loss_sources,
    }

    out_prefix = str(args.output_prefix or "analysis_current_loss_sources_backfilled").strip()
    out_main_latest = logs_dir / f"{out_prefix}_latest.json"
    out_main_stamped = logs_dir / f"{out_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out_by_source_latest = logs_dir / "analysis_loss_by_target_source_backfilled_latest.json"
    out_by_source_stamped = logs_dir / f"analysis_loss_by_target_source_backfilled_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    out_main_latest.write_text(json.dumps(main_out, ensure_ascii=False, indent=2), encoding="utf-8")
    out_main_stamped.write_text(json.dumps(main_out, ensure_ascii=False, indent=2), encoding="utf-8")
    out_by_source_latest.write_text(
        json.dumps(by_source_out, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    out_by_source_stamped.write_text(
        json.dumps(by_source_out, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"generated_at={generated_at}")
    print(f"input={input_path}")
    print(f"unknown_before_count={backfill_summary['unknown_before_count']}")
    print(f"unknown_before_pnl_sum={backfill_summary['unknown_before_pnl_sum']:.6f}")
    print(f"unknown_after_count={backfill_summary['unknown_after_count']}")
    print(f"unknown_after_pnl_sum={backfill_summary['unknown_after_pnl_sum']:.6f}")
    print(f"mapped_count={backfill_summary['mapped_count']}")
    print(f"mapped_pnl_sum={backfill_summary['mapped_pnl_sum']:.6f}")
    print(f"out_main_latest={out_main_latest}")
    print(f"out_by_source_latest={out_by_source_latest}")


if __name__ == "__main__":
    main()
