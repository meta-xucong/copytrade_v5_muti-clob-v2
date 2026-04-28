# copytrade_v5_muti-clob-v2

Polymarket copytrading bot migrated to CLOB V2, with phased gateway refactor, pUSD readiness checks, sandbox validation scripts, and cutover documentation.

## Status

- CLOB client path migrated to V2
- Order reads and writes unified behind `ct_clob_gateway.py`
- V1 `fee_rate_bps` and `/fee-rate` flow removed
- pUSD collateral readiness checks added
- Sandbox trade and restart-recovery smoke scripts included
- Latest local regression at publish time: `79 passed`

## Repository Layout

- `copytrade_run.py`: main runtime and worker supervisor
- `ct_clob_gateway.py`: CLOB V2 adapter for reads, writes, market info, and preflight checks
- `ct_exec.py`: execution and order-management logic
- `ct_state.py`: runtime state, cache, and preflight persistence
- `bounded_copytrade_runner.py`: bounded session runner
- `persistent_copytrade_runner.py`: long-running supervisor runner
- `smartmoney_query/`: local Polymarket Data API helper package required by `copytrade_run.py` and `ct_data.py`
- `sandbox_trade_smoke.py`: live sandbox trade smoke
- `sandbox_restart_recovery_smoke.py`: restart recovery validation
- `tests/`: unit and regression tests
- `doc/`: migration plan, design docs, test plan, and `agents.md`
- `windows/`: Windows launch helpers

## Requirements

- Python 3.11+
- `requests`
- Polygon-compatible funded test or production account
- `py-clob-client-v2`
- pUSD-ready Polymarket funder wallet for live order tests

## Setup

1. Create a virtual environment and install dependencies used by your local workflow.
2. Copy `accounts.example.json` to `accounts.json`.
3. Fill in your own account details.
4. Review and adjust `copytrade_config.json`.
5. For live CLOB V2 trading, make sure the funder wallet has:
   - `pUSD` balance
   - required exchange allowances
   - any conditional token allowances needed for sell-side tests

`accounts.json` is intentionally ignored by git and is never published from this repository.

## Run

Main runtime:

```powershell
python .\copytrade_run.py
```

Bounded session:

```powershell
python .\bounded_copytrade_runner.py
```

Persistent supervisor:

```powershell
python .\persistent_copytrade_runner.py
```

## Sandbox Validation

Trade smoke:

```powershell
python .\sandbox_trade_smoke.py
```

Restart recovery smoke:

```powershell
python .\sandbox_restart_recovery_smoke.py
```

These scripts are intended for staged validation against the CLOB V2 sandbox environment before production cutover.

## Tests

Run the full regression suite:

```powershell
pytest -q
```

Optional syntax check:

```powershell
python -m py_compile copytrade_run.py ct_clob_gateway.py ct_exec.py ct_state.py bounded_copytrade_runner.py persistent_copytrade_runner.py sandbox_trade_smoke.py sandbox_restart_recovery_smoke.py
```

## Documentation

Start here for migration context:

- `doc/README.md`
- `doc/01-requirements-baseline.md`
- `doc/02-gap-analysis-v1-to-v2.md`
- `doc/03-technical-design.md`
- `doc/04-implementation-roadmap.md`
- `doc/05-test-plan-and-cutover-runbook.md`
- `doc/agents.md`

## Safety Notes

- Never commit real private keys or live account files
- Keep `accounts.json` local only
- Treat sandbox and production wallets separately
- Re-run sandbox smoke tests after any order-path change

## Published Version

- Public repository: <https://github.com/meta-xucong/copytrade_v5_muti-clob-v2>
- Release tag target: `v5.0.0-clob-v2`
