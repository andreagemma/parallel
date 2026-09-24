from importlib.metadata import version

import parallel


def test_version_matches_package_metadata() -> None:
    assert parallel.__version__ == version("ga-parallel")
