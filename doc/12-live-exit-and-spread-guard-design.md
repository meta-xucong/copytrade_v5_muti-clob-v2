# Live Exit and Spread Guard Design

## Goal

The live system should avoid two failure modes seen during the V5 long test:

- BUY should not cross a wide book as taker when the configured spread policy says
  the order should remain maker-only.
- SELL should not wait forever when the target position has repeatedly reduced or
  cleared while the local account still has inventory or open exit orders.

## BUY Taker Spread Guard

The hard guard is intentionally BUY-only. SELL, must-exit, hemostasis, and exit
closeout still need their existing liquidation path so a position is not stranded.

Configuration:

- `buy_taker_max_spread`: maximum absolute spread allowed for any BUY taker path.
- `buy_taker_require_known_spread`: when true, BUY taker is blocked if bid/ask
  spread cannot be computed.

Guarded paths:

- normal taker selection from `taker_spread_threshold`;
- small BUY taker override used to avoid maker min-size bumping;
- maker-timeout-to-taker conversion;
- final action construction defense before a BUY action is emitted as taker.

If blocked, the code logs `BUY_TAKER_SPREAD_BLOCK` or `MAKER_TIMEOUT_HOLD` and
keeps maker behavior or skips a tiny BUY that would otherwise need taker.

## SELL Confirmed Drop Exit

The exit decision is intentionally asymmetric: adding exposure needs stricter
filters, while reducing exposure can proceed on stronger evidence.

The system now treats repeated target-position reductions as an exit trigger when
all are true:

- the local account still has shares or open orders;
- the target reduction has appeared for `sell_confirm_max` rounds inside
  `sell_confirm_window_sec`;
- either the target is confirmed at zero, the legacy threshold is met, or the
  configured minimum confirmed drop is met.

Configuration:

- `sell_confirm_exit_on_confirmed_drop`: enable this fallback.
- `sell_confirm_exit_on_target_zero`: target zero always forces exit when local
  exposure exists.
- `sell_confirm_exit_min_drop_shares`: minimum repeated drop in target shares.
- `sell_confirm_exit_min_drop_ratio`: minimum repeated drop relative to the last
  target baseline when a baseline exists.

The promoted exit enters the same must-exit and exit-closeout pipeline as an
explicit SELL, so final success is still based on inventory and open-order
confirmation rather than local state alone.

## Non-goals

- No source-level PnL circuit breaker is introduced.
- BUY topic whitelist and blacklist behavior is unchanged.
- SELL filtering is not tightened; the change only reduces false holds on exit.

