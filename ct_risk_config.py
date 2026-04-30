from __future__ import annotations

import json
from typing import Any, Dict, Mapping, Optional


RISK_SPECS: Dict[str, tuple[str, Optional[float]]] = {
    "max_single_buy_usd": ("max_order_usd", 0.0),
    "max_token_exposure_usd": ("max_position_usd_per_token", 0.0),
    "max_condition_exposure_usd": ("max_position_usd_per_condition", 0.0),
    "max_event_exposure_usd": ("max_position_usd_per_event", 0.0),
    "max_total_exposure_usd": ("max_notional_total", 0.0),
    "max_token_buy_spend_usd": ("max_notional_per_token", None),
    "max_total_buy_spend_usd": ("accumulator_max_total_usd", None),
}

FLAT_TO_RISK: Dict[str, str] = {
    legacy: modern for modern, (legacy, _default) in RISK_SPECS.items()
}


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _has_key(mapping: Mapping[str, Any], key: str) -> bool:
    return key in mapping and mapping.get(key) is not None


def _to_optional_float(value: Any, default: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, str) and value.strip().lower() in {"", "none", "null", "disabled"}:
        return None
    try:
        return float(value)
    except Exception:
        return default


def _read_modern_or_legacy(
    source: Mapping[str, Any],
    modern_key: str,
    legacy_key: str,
    default: Optional[float],
) -> Optional[float]:
    risk = _mapping(source.get("risk"))
    if modern_key in risk:
        return _to_optional_float(risk.get(modern_key), default)
    if modern_key in source:
        return _to_optional_float(source.get(modern_key), default)
    if legacy_key in source:
        return _to_optional_float(source.get(legacy_key), default)
    return default


def effective_risk_config(
    cfg: Mapping[str, Any],
    account_overrides: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Optional[float]]:
    """Return the final modern risk config for a root config plus account overrides.

    New names intentionally distinguish exposure caps from historical buy-spend caps.
    Legacy flat fields remain supported so older config files keep their behavior.
    """
    overrides = _mapping(account_overrides)
    effective: Dict[str, Optional[float]] = {}
    for modern_key, (legacy_key, default) in RISK_SPECS.items():
        value = _read_modern_or_legacy(cfg, modern_key, legacy_key, default)
        risk_overrides = _mapping(overrides.get("risk"))
        if modern_key in risk_overrides:
            value = _to_optional_float(risk_overrides.get(modern_key), value)
        elif modern_key in overrides:
            value = _to_optional_float(overrides.get(modern_key), value)
        elif legacy_key in overrides:
            value = _to_optional_float(overrides.get(legacy_key), value)
        effective[modern_key] = value
    return effective


def normalize_risk_config(
    cfg: Mapping[str, Any],
    account_overrides: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """Return a shallow config copy with modern risk names projected to legacy keys.

    The rest of the codebase still reads the legacy flat keys in many places. This
    function keeps those reads working while allowing configs to use the clearer
    nested ``risk`` object.
    """
    out: Dict[str, Any] = dict(cfg)
    effective = effective_risk_config(cfg, account_overrides)
    out["_risk_effective"] = effective

    def set_legacy(modern_key: str, legacy_key: str, disabled_value: float = 0.0) -> None:
        value = effective.get(modern_key)
        out[legacy_key] = disabled_value if value is None else value

    set_legacy("max_single_buy_usd", "max_order_usd")
    set_legacy("max_token_exposure_usd", "max_position_usd_per_token")
    set_legacy("max_condition_exposure_usd", "max_position_usd_per_condition")
    set_legacy("max_event_exposure_usd", "max_position_usd_per_event")
    set_legacy("max_total_exposure_usd", "max_notional_total")
    set_legacy("max_token_buy_spend_usd", "max_notional_per_token")
    set_legacy("max_total_buy_spend_usd", "accumulator_max_total_usd")
    return out


def account_risk_overrides(account_cfg: Mapping[str, Any]) -> Dict[str, Any]:
    """Extract only account-level risk overrides, avoiding secrets and identity fields."""
    overrides: Dict[str, Any] = {}
    risk = _mapping(account_cfg.get("risk"))
    if risk:
        overrides["risk"] = dict(risk)
    for legacy_key, modern_key in FLAT_TO_RISK.items():
        if legacy_key in account_cfg and modern_key not in overrides.setdefault("risk", {}):
            overrides.setdefault("risk", {})[modern_key] = account_cfg.get(legacy_key)
    for modern_key in RISK_SPECS:
        if modern_key in account_cfg and modern_key not in overrides.setdefault("risk", {}):
            overrides.setdefault("risk", {})[modern_key] = account_cfg.get(modern_key)
    return overrides


def risk_signature(cfg: Mapping[str, Any]) -> str:
    effective = _mapping(cfg.get("_risk_effective")) or effective_risk_config(cfg)
    return json.dumps(effective, sort_keys=True, separators=(",", ":"))


def format_risk_config(cfg: Mapping[str, Any]) -> str:
    effective = _mapping(cfg.get("_risk_effective")) or effective_risk_config(cfg)

    def fmt(key: str) -> str:
        value = effective.get(key)
        return "disabled" if value is None else ("%g" % float(value))

    return (
        f"max_single_buy={fmt('max_single_buy_usd')} "
        f"token_exposure={fmt('max_token_exposure_usd')} "
        f"condition_exposure={fmt('max_condition_exposure_usd')} "
        f"event_exposure={fmt('max_event_exposure_usd')} "
        f"total_exposure={fmt('max_total_exposure_usd')} "
        f"token_buy_spend={fmt('max_token_buy_spend_usd')} "
        f"total_buy_spend={fmt('max_total_buy_spend_usd')}"
    )
