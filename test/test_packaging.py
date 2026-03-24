"""Test packaging configuration and metadata."""
import sys
from importlib.metadata import version, requires

import pytest


def test_version_starts_with_7():
    """Verify package version starts with 7."""
    v = version("hysds-chimera")
    assert v.startswith("7."), f"Expected version 7.x, got {v}"


def test_required_sibling_deps_declared():
    """Verify HySDS sibling dependencies are declared."""
    deps = requires("hysds-chimera")
    assert deps is not None, "No dependencies found"
    
    dep_names = {dep.split()[0].split(";")[0].split(">=")[0].split("~=")[0].split("<")[0]
                 for dep in deps}
    
    required_siblings = {"hysds-core", "hysds-commons", "hysds-sciflo"}
    missing = required_siblings - dep_names
    
    assert not missing, f"Missing required HySDS deps: {missing}"


def test_core_modules_importable():
    """Verify core chimera modules can be imported."""
    import chimera
    assert hasattr(chimera, "__version__")


def test_python_version_requirement():
    """Verify running on Python 3.12+."""
    assert sys.version_info >= (3, 12), "Requires Python 3.12+"


def test_package_name_is_hysds_chimera():
    """Verify package is published as hysds-chimera."""
    v = version("hysds-chimera")
    assert v is not None, "Package 'hysds-chimera' not found"


def test_import_name_is_chimera():
    """Verify import name remains 'chimera' (not hysds_chimera)."""
    import chimera
    assert chimera.__name__ == "chimera"
