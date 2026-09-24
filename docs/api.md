# API Reference

## `Parallel`

`Parallel` is an instance-based runtime. Create one instance for each
independent execution context; instances do not share engine state.

```python
runtime = Parallel(num_cpus=2, engine=Engine.MULTITHREADING)
```

The runtime can also be used as a context manager. It shuts down its backend
when the block exits:

```python
with Parallel(num_cpus=2, engine=Engine.MULTITHREADING) as runtime:
    results = runtime.map_list(fn, tasks)
```

### `Engine`

The public enum used to select the execution backend:

- `Engine.NONE`
- `Engine.MULTITHREADING`
- `Engine.JOBLIB`
- `Engine.DASK`
- `Engine.DASK_MULTITHREADING`
- `Engine.RAY`

`Engine.parse(value)` accepts either an `Engine` member or a string and
normalizes it to the canonical enum value.

### `Parallel.configure`

```python
runtime.configure(num_cpus=None, engine=None, log=None, **kwargs)
```

Configures the runtime instance (starting a local Dask cluster or Ray runtime
as needed) and returns the number of CPUs made available. The `engine` argument
accepts either `Engine` values or strings such as `"ray"` or `"threading"`.
The constructor accepts the same configuration arguments.

### `Parallel.map`

```python
runtime.map(fn, tasks, engine=None, n_workers=None, chunk_size=None, **kwargs)
```

Splits `tasks` into chunks of `chunk_size` (defaults to an even split across
workers) and calls `fn(tasks=chunk, **kwargs)` for each chunk using the active
engine. Returns a generator that yields each chunk's result as it completes.

### `Parallel.apply`

```python
Parallel.apply(fn, params, engine=None, **kwargs)
```

Convenience wrapper around `map` for running `fn` once with a single task
built from `params`.

### `Parallel.map_list`

```python
runtime.map_list(fn, tasks, engine=None, n_workers=None, chunk_size=None, **kwargs)
```

Executes `map` eagerly and returns a flat list. This is useful when the
function returns a list for each chunk and callers need one combined result.

### `Parallel.session`

```python
Parallel.session(num_cpus=None, engine=None, log=None, **kwargs)
```

Creates and returns a configured `Parallel` instance. It is intended for
scoped use with a context manager:

```python
with Parallel.session(num_cpus=2, engine="threading") as runtime:
    results = runtime.map_list(fn, tasks)
```

### `Parallel.shutdown`

```python
runtime.shutdown(force=False)
```

Releases the resources owned by the runtime instance (Ray runtime, Dask
client/cluster, or joblib pool). Use `force=True` to force shutdown even when
the runtime was configured more than once.
