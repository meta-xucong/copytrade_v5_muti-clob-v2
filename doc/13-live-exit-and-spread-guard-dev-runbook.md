# Live Exit and Spread Guard Dev Runbook

## Changed Files

- `ct_exec.py`: BUY taker spread guard.
- `copytrade_run.py`: confirmed target-drop exit promotion.
- `copytrade_config.json`: live defaults for the new guard knobs.
- `tests/test_sell_path_fixes.py`: BUY spread and maker-timeout regression tests.
- `tests/test_reentry_and_source_filter.py`: confirmed target-drop exit tests.

## Validation Checklist

Run before switching live:

```powershell
python -m py_compile copytrade_run.py ct_exec.py ct_state.py ct_runtime_health.py ct_ws_market.py
python -m pytest -q
```

Then run a dry validation using the multi-node entrypoint or an equivalent
single-node dry run if live port contention prevents parallel multi-node dry run.

Expected dry/live log signals:

- `BUY_TAKER_SPREAD_BLOCK` appears for tiny BUYs on wide books.
- `MAKER_TIMEOUT_HOLD` appears instead of switching timed-out BUY maker orders to
  taker when spread is wider than `buy_taker_max_spread`.
- `FORCE ... reason=sell_confirm_target_zero` or
  `FORCE ... reason=sell_confirm_position_drop` appears only after the configured
  confirmation rounds and only when local exposure exists.
- `EXIT_CLOSEOUT_WAIT` may appear while an exit is still confirming; it should
  not finalize until inventory and open orders are clear.

## Live Cutover

1. Keep the current live process running while static tests and dry validation
   complete.
2. Stop V5 live only after validation passes:

```powershell
windows\entrypoints\multi_node_5.ps1 -Action stop -Mode live
```

3. Start the updated live process:

```powershell
windows\entrypoints\multi_node_5.ps1 -Action start -Mode live
```

4. Check status:

```powershell
windows\entrypoints\multi_node_5.ps1 -Action status -Mode live
```

## Stop Conditions During Long Test

Stop V5 live and diagnose if any of these happen:

- any node is not `running` or supervisor restarts increase;
- `Traceback`, `RuntimeError`, `CLOB init failed`, `status=301`, or
  `pUSD preflight failed`;
- BUY taker action appears with spread above `buy_taker_max_spread`;
- BUY exposure obviously exceeds `max_single_buy_usd` or
  `max_position_usd_per_token`;
- SELL, must-exit, or hemostasis is blocked by BUY-only whitelist/blacklist/topic
  filters;
- the same account and token remains in `EXIT_CLOSEOUT_WAIT` long enough to form
  material risk.

Transient network errors, normal whitelist BUY skips, dead books, and isolated
no-best-bid exit attempts are observation items, not automatic stop triggers.

