# ga-parallel

[![PyPI](https://img.shields.io/pypi/v/ga-parallel.svg)](https://pypi.org/project/ga-parallel/)
[![Python](https://img.shields.io/pypi/pyversions/ga-parallel.svg)](https://pypi.org/project/ga-parallel/)

`parallel` provides a single `Parallel` helper class that runs a function over
a collection of tasks using a selectable execution engine: plain threading,
[joblib](https://joblib.readthedocs.io), [dask](https://www.dask.org/), or
[ray](https://www.ray.io/). The engine is chosen at runtime, so the same code
can scale from a laptop to a cluster without changes.

The PyPI distribution is named `ga-parallel`; the import package is named
`parallel`.

## Installation

```bash
python -m pip install ga-parallel
```

`dask`, `ray`, and `joblib` are optional third-party engines and are not
installed by default. Install only the engines you need as extras:

```bash
python -m pip install "ga-parallel[dask]"
python -m pip install "ga-parallel[ray]"
python -m pip install "ga-parallel[joblib]"
```

Install every optional engine at once with:

```bash
python -m pip install "ga-parallel[all]"
```

Development and test tools are available as extras:

```bash
python -m pip install -e ".[test]"
python -m pip install -e ".[dev]"
```

## Quick Start

```python
from parallel import Engine, Parallel


def double(tasks: list[dict]) -> list[int]:
    return [task["value"] * 2 for task in tasks]


tasks = [{"value": i} for i in range(10)]

runtime = Parallel(num_cpus=2, engine=Engine.MULTITHREADING)
results = runtime.map_list(double, tasks)

runtime.shutdown()
```

`engine` may also be passed as a string, for example `"threading"` or `"ray"`.
The library normalizes it via `Engine.parse(...)`.

For scoped execution, use the runtime as a context manager:

```python
with Parallel(num_cpus=2, engine=Engine.MULTITHREADING) as runtime:
  results = runtime.map_list(double, tasks)
```

## Engines

Each `Parallel` instance owns its engine state and supports the following
engines, selected via the `engine` argument of the constructor, `configure(...)`
or `map(...)`:

- `Engine.NONE` - sequential execution (default with a single CPU).
- `Engine.MULTITHREADING` - `concurrent.futures.ThreadPoolExecutor`.
- `Engine.JOBLIB` - `joblib.Parallel` with the `loky` backend
  (requires the `joblib` extra).
- `Engine.DASK` - a local Dask `distributed` cluster with
  multi-process workers (requires the `dask` extra).
- `Engine.DASK_MULTITHREADING` - a local Dask `distributed` client
  with a single multi-threaded worker (requires the `dask` extra).
- `Engine.RAY` - a local or remote Ray cluster (requires the `ray` extra).

If an engine's dependency is missing, `Parallel` falls back to
`Engine.MULTITHREADING` and logs a warning.

## API Overview

- `Parallel(num_cpus=None, engine=None, log=None, **kwargs)` creates and
  configures an independent runtime instance.
- `Parallel.configure(num_cpus=None, engine=None, log=None, **kwargs)`
  configures that instance and returns the number of usable CPUs.
- `Parallel.map(fn, tasks, engine=None, n_workers=None, chunk_size=None, **kwargs)`
  splits `tasks` into chunks and yields the result of `fn` for each chunk.
- `Parallel.map_list(fn, tasks, engine=None, n_workers=None, chunk_size=None,
  **kwargs)` eagerly executes `map` and returns a flat list of results.
- `Parallel.apply(fn, params, engine=None, **kwargs)` runs `fn` once with a
  single task payload.
- `Parallel.session(...)` creates a configured instance suitable for a
  `with` block.
- `Parallel.shutdown(force=False)` releases the resources owned by that
  instance.
