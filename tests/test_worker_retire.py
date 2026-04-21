from copytrade_run import WORKER_RETIRED_EXIT_CODE, _resolve_effective_worker_shard_count


def test_resolve_effective_worker_shard_count_keeps_parallelism_when_accounts_exist() -> None:
    active_workers, retire_worker = _resolve_effective_worker_shard_count(
        valid_account_count=2,
        worker_count=2,
        worker_index=1,
    )
    assert active_workers == 2
    assert retire_worker is False


def test_resolve_effective_worker_shard_count_retires_empty_shard_after_init_shrink() -> None:
    active_workers, retire_worker = _resolve_effective_worker_shard_count(
        valid_account_count=1,
        worker_count=2,
        worker_index=1,
    )
    assert active_workers == 1
    assert retire_worker is True


def test_worker_retired_exit_code_is_stable() -> None:
    assert WORKER_RETIRED_EXIT_CODE == 88
