# Changelog

All notable changes to this project will be documented in this file.

## Unreleased

## 0.1.2 - 2026-10-27

- Preserved the public `threading` engine value and the default CPU-count
  behavior.
- Documented the public runtime methods and debug/error handling options.
- Fixed sequential recovery of unfinished parallel tasks and made `ignore`
  consistently suppress task errors.
- Rejected non-positive chunk sizes with a clear `ValueError`.

## 0.1.1

- Documented the instance-based `Parallel` runtime API.
- Added usage examples for `map_list`, `session`, and context-manager cleanup.
- Clarified that each runtime instance owns its backend resources independently.

## 0.1.0 - 2026-09-23

- Prepared the package for publishing as `ga-parallel`.
- Added project metadata (`pyproject.toml`), `LICENSE`, `MANIFEST.in`,
  `README.md`, and `src/parallel/_version.py`.
- Added `docs/` with an overview and API reference.
- Made the `dask`, `ray`, and `joblib` execution engines optional
  dependencies, installable via the `dask`, `ray`, `joblib`, or `all` extras.
- Added third-party licensing artifacts: `THIRD_PARTY_NOTICES.md`,
  `licenses/third_party/summary.tsv`, and archived package license files
  under `licenses/third_party/packages/`.
- Added unit tests under `tests/`.
