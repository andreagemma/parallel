import multiprocessing

import pytest

from parallel import Engine, Parallel
from tests._helpers import double_tasks


def _double(tasks: list[dict]) -> list[int]:
    return double_tasks(tasks)


@pytest.fixture(autouse=True)
def _reset_parallel_state() -> None:  # type: ignore
    yield


def test_get_num_cpus_defaults_to_all_but_one() -> None:
    assert Parallel._get_num_cpus() == max(1, multiprocessing.cpu_count() - 1)


def test_get_num_cpus_accepts_absolute_int() -> None:
    assert Parallel._get_num_cpus(1) == 1


def test_get_num_cpus_accepts_relative_fraction() -> None:
    total = multiprocessing.cpu_count()
    result = Parallel._get_num_cpus(0.5)
    assert 1 <= result <= total


def test_engine_parse_accepts_enum_and_string() -> None:
    assert Engine.parse(Engine.DASK) is Engine.DASK
    assert Engine.parse("ray") is Engine.RAY
    assert Engine.parse("DASK_MULTITHREADING") is Engine.DASK_MULTITHREADING


def test_map_sequential_without_engine() -> None:
    runtime = Parallel()
    results = list(runtime.map(_double, [{"value": 1}, {"value": 2}]))
    assert results == [[2, 4]]


def test_map_multithreading_engine() -> None:
    runtime = Parallel(num_cpus=2, engine=Engine.MULTITHREADING)
    tasks = [{"value": i} for i in range(4)]
    results = []
    for chunk in runtime.map(_double, tasks):
        results.extend(chunk)
    runtime.shutdown(force=True)
    assert sorted(results) == [0, 2, 4, 6]


def test_map_list_returns_flattened_results() -> None:
    runtime = Parallel(num_cpus=2, engine=Engine.MULTITHREADING)
    tasks = [{"value": i} for i in range(4)]
    result = runtime.map_list(_double, tasks)
    runtime.shutdown(force=True)
    assert result == [0, 2, 4, 6]


def test_apply_single_task() -> None:
    runtime = Parallel(num_cpus=2, engine=Engine.MULTITHREADING)
    result = list(runtime.apply(_double, {"value": 3}))
    runtime.shutdown(force=True)
    assert result == [[6]]


@pytest.mark.parametrize("engine", [Engine.RAY, Engine.DASK, Engine.JOBLIB])
def test_map_with_supported_engines_or_threading_fallback(engine: Engine) -> None:
    runtime = Parallel(num_cpus=2, engine=engine)
    tasks = [{"value": i} for i in range(4)]
    results = []
    for chunk in runtime.map(_double, tasks):
        results.extend(chunk)
    runtime.shutdown(force=True)
    assert sorted(results) == [0, 2, 4, 6]


def test_two_ray_runtime_instances() -> None:
    pytest.importorskip("ray")
    first_runtime = Parallel(num_cpus=2, engine=Engine.RAY)
    second_runtime = Parallel(num_cpus=20, engine=Engine.RAY)
    try:
        assert first_runtime._parallel_engine is Engine.RAY
        assert second_runtime._parallel_engine is Engine.RAY
        assert first_runtime.map_list(_double, [{"value": 1}] * 10000) == [2] * 10000
        assert second_runtime.map_list(_double, [{"value": 2}] * 10000) == [4] * 10000
    finally:
        second_runtime.shutdown(force=True)
        first_runtime.shutdown(force=True)


def test_two_dask_runtime_instances() -> None:
    pytest.importorskip("dask")
    first_runtime = Parallel(num_cpus=2, engine=Engine.DASK)
    second_runtime = Parallel(num_cpus=20, engine=Engine.DASK)
    try:
        assert first_runtime._parallel_engine is Engine.DASK
        assert second_runtime._parallel_engine is Engine.DASK
        assert first_runtime.map_list(_double, [{"value": 1}] * 10000) == [2] * 10000
        assert second_runtime.map_list(_double, [{"value": 2}] * 10000) == [4] * 10000
    finally:
        second_runtime.shutdown(force=True)
        first_runtime.shutdown(force=True)


def test_two_joblib_runtime_instances() -> None:
    pytest.importorskip("joblib")
    first_runtime = Parallel(num_cpus=2, engine=Engine.JOBLIB)
    second_runtime = Parallel(num_cpus=20, engine=Engine.JOBLIB)
    try:
        assert first_runtime._parallel_engine is Engine.JOBLIB
        assert second_runtime._parallel_engine is Engine.JOBLIB
        assert first_runtime.map_list(_double, [{"value": 1}] * 10000) == [2] * 10000
        assert second_runtime.map_list(_double, [{"value": 2}] * 10000) == [4] * 10000
    finally:
        second_runtime.shutdown(force=True)
        first_runtime.shutdown(force=True)


def test_two_multithreading_runtime_instances() -> None:
    first_runtime = Parallel(num_cpus=2, engine=Engine.MULTITHREADING)
    second_runtime = Parallel(num_cpus=20, engine=Engine.MULTITHREADING)
    try:
        assert first_runtime._parallel_engine is Engine.MULTITHREADING
        assert second_runtime._parallel_engine is Engine.MULTITHREADING
        assert first_runtime.map_list(_double, [{"value": 1}] * 10000) == [2] * 10000
        assert second_runtime.map_list(_double, [{"value": 2}] * 10000) == [4] * 10000
    finally:
        second_runtime.shutdown(force=True)
        first_runtime.shutdown(force=True)


def test_two_none_runtime_instances() -> None:
    first_runtime = Parallel(num_cpus=2, engine=Engine.NONE)
    second_runtime = Parallel(num_cpus=20, engine=Engine.NONE)
    try:
        assert first_runtime._parallel_engine is Engine.NONE
        assert second_runtime._parallel_engine is Engine.NONE
        assert first_runtime.map_list(_double, [{"value": 1}] * 10000) == [2] * 10000
        assert second_runtime.map_list(_double, [{"value": 2}] * 10000) == [4] * 10000
    finally:
        second_runtime.shutdown(force=True)
        first_runtime.shutdown(force=True)


def test_two_dask_multithreading_runtime_instances() -> None:
    pytest.importorskip("dask")
    first_runtime = Parallel(num_cpus=2, engine=Engine.DASK_MULTITHREADING)
    second_runtime = Parallel(num_cpus=20, engine=Engine.DASK_MULTITHREADING)
    try:
        assert first_runtime._parallel_engine is Engine.DASK_MULTITHREADING
        assert second_runtime._parallel_engine is Engine.DASK_MULTITHREADING
        assert first_runtime.map_list(_double, [{"value": 1}] * 10000) == [2] * 10000
        assert second_runtime.map_list(_double, [{"value": 2}] * 10000) == [4] * 10000
    finally:
        second_runtime.shutdown(force=True)
        first_runtime.shutdown(force=True)


def test_session_context_manager_configures_and_shutdowns() -> None:
    with Parallel(num_cpus=2, engine=Engine.MULTITHREADING) as runtime:
        assert runtime._num_cpus > 0
        assert runtime._parallel_engine is Engine.MULTITHREADING

    assert runtime._parallel_engine is Engine.NONE
