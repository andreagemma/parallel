"""Pluggable parallel task execution engine (threading, joblib, dask, ray)."""

from __future__ import annotations

import importlib
import logging
import multiprocessing
import os
from collections.abc import Callable, Generator, Iterable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from enum import Enum
from typing import Any

from serializer import Serializer


_ray_runtime_users = 0


class Engine(str, Enum):
    """Supported execution engines for parallel job dispatch."""

    RAY = "ray"
    DASK = "dask"
    DASK_MULTITHREADING = "dask_multithreading"
    NONE = "none"
    MULTITHREADING = "threading"
    JOBLIB = "joblib"

    @classmethod
    def parse(cls, value: Engine | str) -> Engine:
        """Normalize an engine value from either an enum member or a string."""
        if isinstance(value, cls):
            return value
        if not isinstance(value, str):
            raise TypeError(f"Unsupported engine value: {value!r}")

        normalized = value.strip().lower().replace("-", "_").replace(" ", "_")
        for member in cls:
            if normalized == member.value or normalized == member.name.lower():
                return member
        if normalized in {"multi", "multithread", "thread", "threading"}:
            return cls.MULTITHREADING
        raise ValueError(f"Unsupported engine value: {value!r}")


class Parallel:
    """Runs a function over chunks of tasks using a selectable execution engine.

    The runtime is instance-based: each object owns its engine state, CPU count,
    logger, and initialized backends. This keeps multiple sessions independent
    while still allowing a scoped ``with Parallel(...)`` usage pattern.
    """

    ENGINE_RAY = Engine.RAY
    ENGINE_DASK = Engine.DASK
    ENGINE_DASK_MULTITHREADING = Engine.DASK_MULTITHREADING
    ENGINE_NONE = Engine.NONE
    ENGINE_MULTITHREADING = Engine.MULTITHREADING
    ENGINE_JOBLIB = Engine.JOBLIB

    def __init__(
        self,
        num_cpus: int | None = None,
        engine: Engine | str | None = None,
        log: logging.Logger | None = None,
        **kwargs: Any,
    ) -> None:
        """Create a parallel runtime bound to a single process-local configuration."""
        self._ray_initialized = False
        self._dask_initialized = False
        self._dask_client: Any | None = None
        self._num_cpus = 1
        self._parallel_engine = Engine.NONE
        self._initialzations = 0
        self._initializations = 0
        self._dask_cluster: Any | None = None
        self._log: logging.Logger | None = None
        self._joblib_engine: Any = None
        self._joblib_initialized = False
        self._initialized = False
        self._log = self._get_logger(log)
        if num_cpus is not None or engine is not None or kwargs:
            self.configure(num_cpus=num_cpus, engine=engine, log=log, **kwargs)

    def __enter__(self) -> Parallel:
        """Return the configured runtime as a context manager target."""
        return self

    def __exit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> None:
        """Shutdown the runtime when leaving a context block."""
        self.shutdown(force=True)

    @staticmethod
    def _get_num_cpus(num_cpus: float | None = None) -> int:
        """Resolve the number of CPUs to use from an absolute or relative value."""
        total_cpu_count = multiprocessing.cpu_count()
        if num_cpus is None:
            num_cpus_final = int(total_cpu_count - 1)
        elif isinstance(num_cpus, float):
            if 0 < num_cpus < 1:
                num_cpus_final = int(num_cpus * total_cpu_count)
            elif -1 < num_cpus < 0:
                num_cpus_final = int((1 + num_cpus) * total_cpu_count)
            else:
                num_cpus = int(num_cpus)
                num_cpus_final = int(num_cpus)
        elif isinstance(num_cpus, int):
            if num_cpus <= 0:
                num_cpus_final = int(total_cpu_count + num_cpus)
            else:
                num_cpus_final = int(num_cpus)
        else:
            num_cpus_final = int(total_cpu_count - 1)

        num_cpus_final = max(1, int(num_cpus_final))
        num_cpus_final = min(num_cpus_final, total_cpu_count)
        return num_cpus_final

    @staticmethod
    def _get_num_min_cpus(num_cpus: float | None = None) -> int:
        """Resolve the number of CPUs to use, bounded by the active engine's CPUs."""
        max_cpus = multiprocessing.cpu_count()
        if num_cpus is None:
            num_cpus_final = int(max_cpus - 1)
        elif isinstance(num_cpus, float):
            if 0 < num_cpus < 1:
                num_cpus_final = int(num_cpus * max_cpus)
            elif -1 < num_cpus < 0:
                num_cpus_final = int((1 + num_cpus) * max_cpus)
            else:
                num_cpus = int(num_cpus)
                num_cpus_final = int(num_cpus)
        elif isinstance(num_cpus, int):
            if num_cpus <= 0:
                num_cpus_final = int(max_cpus + num_cpus)
            else:
                num_cpus_final = int(num_cpus)
        else:
            num_cpus_final = int(max_cpus - 1)

        num_cpus_final = max(1, int(num_cpus_final))
        num_cpus_final = min(num_cpus_final, max_cpus)
        return num_cpus_final

    def _get_logger(self, log: logging.Logger | None = None) -> logging.Logger:
        """Return the runtime logger, creating one if needed."""
        if self._log is not None:
            return self._log

        logger = log or logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        logger.propagate = False

        if not any(isinstance(handler, logging.StreamHandler) for handler in logger.handlers):
            handler = logging.StreamHandler()
            handler.setLevel(logging.INFO)
            handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
            logger.addHandler(handler)

        self._log = logger
        return self._log

    def configure(
        self,
        num_cpus: int | None = None,
        engine: Engine | str | None = None,
        log: logging.Logger | None = None,
        **kwargs: Any,
    ) -> int:
        """Configure the execution engine for this runtime instance."""
        self._log = self._get_logger(log)

        if engine is not None:
            engine = Engine.parse(engine)
        else:
            engine = Engine.NONE

        if self._initializations > 0:
            self._log.warning(
                f"Parallel engine already initialized ({self._num_cpus} CPUs available with {self._parallel_engine.value} engine)."
            )
            self._initializations += 1
            self._initialzations = self._initializations
            return self._num_cpus

        self._num_cpus = self._get_num_cpus(num_cpus)
        self._initializations += 1
        self._initialzations = self._initializations

        if engine == Engine.RAY:
            try:
                import ray  # pyright: ignore[reportMissingImports]

                ray.util.register_serializer(dict, serializer=Serializer.dumps, deserializer=Serializer.loads)
                os.environ["RAY_COLOR_PREFIX"] = "1"
            except ImportError:
                pass
            if importlib.util.find_spec("ray") is None:
                self._log.warning("Ray is not installed. Please install it using 'pip install ray'")
                engine = Engine.MULTITHREADING
                self._log.warning("Switching to multi-threaded mode.")

        if engine in (Engine.DASK, Engine.DASK_MULTITHREADING):
            try:
                import dask  # pyright: ignore[reportMissingImports]
                from dask.distributed import Client  # pyright: ignore[reportMissingImports]
            except ImportError:
                pass
            if importlib.util.find_spec("dask") is None:
                self._log.warning("Dask is not installed. Please install it using 'pip install dask'")
                engine = Engine.MULTITHREADING
                self._log.warning("Switching to multi-threaded mode.")

        if engine == Engine.JOBLIB:
            try:
                import joblib
            except ImportError:
                self._log.warning("Joblib is not installed. Please install it using 'pip install joblib'")
                engine = Engine.MULTITHREADING
                self._log.warning("Switching to multi-threaded mode.")

        self._parallel_engine = engine

        if self._num_cpus == 1:
            self._parallel_engine = Engine.NONE
            return self._num_cpus

        if self._parallel_engine == Engine.RAY and self._num_cpus > 1:
            if importlib.util.find_spec("ray") is None:
                raise ImportError("Ray is not installed. Please install it using 'pip install ray'")
            if not self._ray_initialized:
                try:
                    import ray  # pyright: ignore[reportMissingImports]

                    ray.util.register_serializer(dict, serializer=Serializer.dumps, deserializer=Serializer.loads)
                    global _ray_runtime_users
                    if ray.is_initialized():
                        new_num_cpus = int(ray.cluster_resources().get("CPU", self._num_cpus))
                        if new_num_cpus > self._num_cpus:
                            self._log.warning(f"Ray cluster has more CPUs ({new_num_cpus}) than requested ({self._num_cpus}). Adjusting accordingly.")                            
                        else:
                            self._num_cpus = new_num_cpus                        
                        self._ray_initialized = True
                        self._initialized = True
                        _ray_runtime_users += 1
                        return self._num_cpus

                    kwargs["ignore_reinit_error"] = True
                    kwargs["include_dashboard"] = {"log_to_driver": True}
                    address: str | None = kwargs.pop("address", None)
                    if address and address.lower() != "local":
                        try:
                            ray.init(address=address, **kwargs)
                        except ConnectionError:
                            self._log.error("Ray is not initialized. Please check the address and try again.")
                            kwargs.pop("address")
                            ray.init(address="local", num_cpus=self._num_cpus, **kwargs)
                            self._log.warning("Ray is initialized with default settings.")
                    else:
                        ray.init(address="local", num_cpus=self._num_cpus)
                    self._num_cpus = int(ray.cluster_resources()["CPU"])
                    self._ray_initialized = True
                    self._initialized = True
                    _ray_runtime_users += 1
                except Exception:
                    self._parallel_engine = Engine.MULTITHREADING
                    self._log.error("Error during ray initialization.", exc_info=True)
                    self._log.warning("Ray is not initialized. Switching to multi-threaded mode.")
                    self._ray_initialized = False
                    self._initialized = False
                return self._num_cpus
            return ray.available_resources()["CPU"]

        if self._parallel_engine == Engine.DASK and self._num_cpus > 1:
            if importlib.util.find_spec("dask") is None:
                raise ImportError("Dask is not installed. Please install it using 'pip install dask'")
            if not self._dask_initialized:
                try:
                    import dask  # pyright: ignore[reportMissingImports]
                    from dask.distributed import Client, LocalCluster  # pyright: ignore[reportMissingImports]

                    logging.getLogger("distributed").setLevel(logging.WARNING)
                    logging.getLogger("dask").setLevel(logging.WARNING)

                    self._dask_cluster = LocalCluster(
                        n_workers=self._num_cpus,
                        threads_per_worker=1,
                        memory_limit="auto",
                        processes=True,
                        silence_logs=logging.ERROR,
                    )
                    self._dask_client = Client(self._dask_cluster)
                    self._dask_initialized = True
                    self._initialized = True
                except Exception:
                    self._parallel_engine = Engine.MULTITHREADING
                    self._log.error("Error during dask initialization.", exc_info=True)
                    self._log.warning("Dask is not initialized. Switching to multi-threaded mode.")
                    self._dask_initialized = False
                    self._initialized = False
                return self._num_cpus
            return len(self._dask_client.nthreads())

        if self._parallel_engine == Engine.DASK_MULTITHREADING and self._num_cpus > 1:
            if importlib.util.find_spec("dask") is None:
                raise ImportError("Dask is not installed. Please install it using 'pip install dask'")
            if not self._dask_initialized:
                try:
                    import dask  # pyright: ignore[reportMissingImports]
                    from dask.distributed import Client  # pyright: ignore[reportMissingImports]

                    logging.getLogger("distributed").setLevel(logging.WARNING)
                    logging.getLogger("dask").setLevel(logging.WARNING)

                    self._dask_client = Client(processes=False, threads_per_worker=self._num_cpus, n_workers=1)
                    self._dask_initialized = True
                    self._initialized = True
                except Exception:
                    self._parallel_engine = Engine.MULTITHREADING
                    self._log.error("Error during dask initialization.", exc_info=True)
                    self._log.warning("Dask is not initialized. Switching to multi-threaded mode.")
                    self._dask_initialized = False
                    self._initialized = False
                return self._num_cpus
            return len(self._dask_client.nthreads())

        if self._parallel_engine == Engine.MULTITHREADING and self._num_cpus > 1:
            self._initialized = True
            return self._num_cpus

        if self._parallel_engine == Engine.JOBLIB and self._num_cpus > 1:
            try:
                import joblib

                self._joblib_engine = joblib.Parallel(n_jobs=self._num_cpus, backend="loky")
                self._joblib_initialized = True
                self._initialized = True
                return self._num_cpus
            except ImportError:
                self._parallel_engine = Engine.MULTITHREADING
                self._log.error("Joblib is not installed. Please install it using 'pip install joblib'")
                self._log.warning("Switching to multi-threaded mode.")
                self._joblib_initialized = False
                self._initialized = False
                return self._num_cpus

        self._num_cpus = 1
        self._initialized = False
        return 1

    def shutdown(self, force: bool = False) -> None:
        """Release the resources owned by this runtime instance."""
        if self._initializations > 0:
            self._initializations -= 1
            self._initialzations = self._initializations
        if self._initializations > 0 and not force:
            self._log.warning("Parallel engine not shutdown. Parallel engine is still in use.")
            return

        if self._parallel_engine == Engine.RAY and self._ray_initialized:
            try:
                import ray  # pyright: ignore[reportMissingImports]

                global _ray_runtime_users
                _ray_runtime_users = max(0, _ray_runtime_users - 1)
                if _ray_runtime_users == 0 and ray.is_initialized():
                    ray.shutdown()
            except Exception:
                pass
            self._ray_initialized = False

        elif self._parallel_engine in [Engine.DASK, Engine.DASK_MULTITHREADING] and self._dask_initialized:
            try:
                if self._dask_client is not None:
                    self._dask_client.close()
            except Exception:
                pass
            self._dask_client = None
            self._dask_initialized = False

        elif self._parallel_engine == Engine.JOBLIB and self._joblib_initialized:
            try:
                if self._joblib_engine is not None:
                    self._joblib_engine.close()
            except Exception:
                pass
            self._joblib_engine = None
            self._joblib_initialized = False

        self._parallel_engine = Engine.NONE
        self._num_cpus = 1
        self._dask_client = None
        self._dask_cluster = None
        self._joblib_engine = None
        self._initializations = 0
        self._initialzations = 0
        self._initialized = False

    @classmethod
    def session(
        cls,
        num_cpus: int | None = None,
        engine: Engine | str | None = None,
        log: logging.Logger | None = None,
        **kwargs: Any,
    ) -> Parallel:
        """Construct and return a configured runtime instance for a ``with`` block."""
        return cls(num_cpus=num_cpus, engine=engine, log=log, **kwargs)

    def map(
        self,
        fn: Callable[..., Any],
        tasks: Iterable[dict],
        engine: Engine | str | None = None,
        n_workers: int | None = None,
        chunk_size: int | None = None,
        **kwargs: Any,
    ) -> Generator[Any, None, None]:
        """Run `fn` over chunks of `tasks` using the active execution runtime."""
        tasks = list(tasks)

        runtime_engine = Engine.parse(engine) if engine is not None else self._parallel_engine
        if engine is not None or n_workers is not None:
            if not self._initialized:
                self.configure(num_cpus=n_workers, engine=runtime_engine)

        if self._parallel_engine == Engine.NONE and runtime_engine != Engine.NONE:
            self._parallel_engine = runtime_engine

        engine_value = self._parallel_engine if self._parallel_engine != Engine.NONE else runtime_engine
        if engine_value is None or engine_value == Engine.NONE:
            n_workers = None
        if n_workers is not None:
            num_cpus = self._get_num_min_cpus(n_workers)
            if num_cpus != self._num_cpus and num_cpus == 1:
                self._log.warning("n_workers = 1. Using single CPU in a single thread mode.")
        else:
            num_cpus = self._num_cpus

        chunk_size = int(max(1, len(tasks) // (num_cpus))) if chunk_size is None else chunk_size
        pair_chunks = [tasks[i : i + chunk_size] for i in range(0, len(tasks), chunk_size)]

        if engine_value == Engine.RAY and num_cpus > 1 and self._ray_initialized:
            import ray  # pyright: ignore[reportMissingImports]

            ray.util.register_serializer(dict, serializer=Serializer.dumps, deserializer=Serializer.loads)
            pair_chunks_refs = [ray.put(chunk) for chunk in pair_chunks]

            @ray.remote
            def calculate(*args: Any, **kwargs: Any) -> Any:
                """Invoke `fn` for a single chunk of tasks on a Ray worker."""
                return fn(*args, **kwargs)

            ref_kwargs: dict[str, Any] = {}
            for key, value in kwargs.items():
                if isinstance(value, (float, int, str, bool, complex)):
                    ref_kwargs[key] = value
                else:
                    ref_kwargs[key] = ray.put(value)

            self._log.debug(f"Run task on engine Ray with {num_cpus} workers.")
            result_ids: list[Any] = [calculate.remote(tasks=chunk_ref, **ref_kwargs) for chunk_ref in pair_chunks_refs]

            while result_ids:
                done_ids, result_ids = ray.wait(result_ids)
                for done_id in done_ids:
                    yield ray.get(done_id)

        elif engine_value in [Engine.DASK, Engine.DASK_MULTITHREADING] and num_cpus > 1 and self._dask_initialized:
            import dask  # pyright: ignore[reportMissingImports]

            @dask.delayed
            def calculate(*args: Any, **kwargs: Any) -> Any:
                """Invoke `fn` for a single chunk of tasks on a Dask worker."""
                return fn(*args, **kwargs)

            self._log.debug(f"Run task on engine Dask with {num_cpus} workers.")
            delayed_results = [calculate(tasks=chunk, **kwargs) for chunk in pair_chunks]
            results: tuple[Any, ...] = dask.compute(*delayed_results, scheduler="processes")
            for value in results:
                yield value

        elif engine_value == Engine.MULTITHREADING and num_cpus > 1:
            def calculate(*args: Any, **kwargs: Any) -> Any:
                """Invoke `fn` for a single chunk of tasks on a worker thread."""
                return fn(*args, **kwargs)

            self._log.debug(f"Run task on engine Multithreading with {num_cpus} workers.")
            with ThreadPoolExecutor(max_workers=num_cpus) as executor:
                futures: dict[Future[Any], list[dict]] = {
                    executor.submit(calculate, tasks=chunk, **kwargs): chunk for chunk in pair_chunks
                }
                for future in as_completed(futures):
                    yield future.result()

        elif engine_value == Engine.JOBLIB and num_cpus > 1:
            import joblib

            def calculate(*args: Any, **kwargs: Any) -> Any:
                """Invoke `fn` for a single chunk of tasks on a joblib worker."""
                return fn(*args, **kwargs)

            self._log.debug(f"Run task on engine Joblib with {num_cpus} workers.")
            results: Any = self._joblib_engine(joblib.delayed(calculate)(tasks=chunk, **kwargs) for chunk in pair_chunks)
            for value in results:
                yield value
        else:
            yield fn(tasks=tasks, **kwargs)

    def map_list(
        self,
        fn: Callable[..., Any],
        tasks: Iterable[dict],
        engine: Engine | str | None = None,
        n_workers: int | None = None,
        chunk_size: int | None = None,
        **kwargs: Any,
    ) -> list[Any]:
        """Run `fn` over chunks of `tasks` and return all results as a flat list."""
        results: list[Any] = []
        for chunk_result in self.map(
            fn=fn,
            tasks=tasks,
            engine=engine,
            n_workers=n_workers,
            chunk_size=chunk_size,
            **kwargs,
        ):
            if isinstance(chunk_result, list):
                results.extend(chunk_result)
            else:
                results.append(chunk_result)
        return results

    def apply(
        self,
        fn: Callable[..., Any],
        params: dict,
        engine: Engine | str | None = None,
        **kwargs: Any,
    ) -> Generator[Any, None, None]:
        """Run `fn` once with a single task built from `params`."""
        return self.map(fn, [params], engine=engine, n_workers=None, chunk_size=1, **kwargs)

    def execute(
        self,
        fn: Callable[..., Any],
        tasks: Iterable[dict],
        engine: Engine | str | None = None,
        n_workers: int | None = None,
        chunk_size: int | None = None,
        **kwargs: Any,
    ) -> Generator[Any, None, None]:
        """Alias for `map` retained for convenience."""
        return self.map(fn, tasks, engine=engine, n_workers=n_workers, chunk_size=chunk_size, **kwargs)

    def initialize_parallel(
        self,
        num_cpus: int | None = None,
        engine: Engine | str | None = None,
        log: logging.Logger | None = None,
        **kwargs: Any,
    ) -> int:
        """Alias for `configure` retained for convenience."""
        return self.configure(num_cpus=num_cpus, engine=engine, log=log, **kwargs)

    def shutdown_parallel(self, force: bool = False) -> None:
        """Alias for `shutdown` retained for convenience."""
        self.shutdown(force=force)

    def run(
        self,
        fn: Callable[..., Any],
        params: dict,
        engine: Engine | str | None = None,
        **kwargs: Any,
    ) -> Generator[Any, None, None]:
        """Apply `fn` to a single payload and return the single chunk result."""
        return self.apply(fn, params, engine=engine, **kwargs)
