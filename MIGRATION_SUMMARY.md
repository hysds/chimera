# HySDS Chimera Packaging Migration Summary

## Migration Completed: March 24, 2026

This document summarizes the migration of the `hysds-chimera` repository from legacy `setup.py` to modern `pyproject.toml` packaging.

---

## Changes Made

### ✅ Files Created

1. **`pyproject.toml`** - Modern packaging configuration
   - Package name: `hysds-chimera` (PyPI) / `chimera` (import)
   - Version: Dynamic from git tags via `hatch-vcs`
   - Dependencies: 4 third-party packages + 3 HySDS siblings
   - Added missing dependencies: `hysds-core~=7.0`, `hysds-commons~=7.0`, `hysds-sciflo~=7.0`

2. **`.github/workflows/publish.yml`** - PyPI publishing automation
   - Triggered on git tags (`v*`)
   - Uses PyPI Trusted Publishers (OIDC)

### ✅ Files Modified

1. **`chimera/__init__.py`**
   - Added `__version__ = version("hysds-chimera")` to previously empty file

2. **`setup.py`**
   - Replaced with minimal shim for backward compatibility
   - Delegates all configuration to `pyproject.toml`
   - Will be removed in v7.1.0+

---

## Key Dependency Changes

### Fixed Issues

| Issue | Before | After |
|-------|--------|-------|
| Missing hysds-core | Not declared | `hysds-core~=7.0` |
| Missing hysds-commons | Not declared | `hysds-commons~=7.0` |
| Missing hysds-sciflo | Not declared | `hysds-sciflo~=7.0` |

### Dependencies Preserved Exactly

All 4 core dependencies maintained with exact pins from original `setup.py`:
- `elasticsearch>=7.0.0,<7.14.0`
- `elasticsearch-dsl>=7.0.0,<=7.4.0`
- `requests>=2.18.4`
- `simplejson>=3.11.1`

---

## Build Verification

```bash
$ python -m build
Successfully built hysds_chimera-3.0.0.post1.dev0+gd83463b0b.d20260324.tar.gz
Successfully built hysds_chimera-3.0.0.post1.dev0+gd83463b0b.d20260324-py3-none-any.whl
```

---

## Next Steps

### Before Publishing to PyPI

1. **Verify sibling packages published first**
   - ✅ `hysds-core~=7.0` must be on PyPI
   - ✅ `hysds-commons~=7.0` must be on PyPI
   - ✅ `hysds-sciflo~=7.0` must be on PyPI

2. **Tag version 7.0.0**
   ```bash
   git tag -a v7.0.0 -m "Release 7.0.0 - Modern packaging migration"
   git push origin v7.0.0
   ```

3. **Configure PyPI Trusted Publisher**
   - Go to https://pypi.org/manage/account/publishing/
   - Add GitHub Actions publisher for `hysds/hysds-chimera` repo
   - Workflow: `publish.yml`
   - Environment: `pypi`

### Installation Methods

#### Development (Local)
```bash
pip install -e .
```

#### Development (From Git Branch)
```bash
pip install "git+https://github.com/hysds/hysds-chimera.git@feature-branch"
```

#### Production (After PyPI Publishing)
```bash
pip install hysds-chimera
```

---

## Backward Compatibility

### Import Names (Unchanged)
```python
# All existing imports continue to work
import chimera
from chimera.adaptation import Adapter
```

### Package Name (Already Prefixed)
- **PyPI package**: `chimera` → `hysds-chimera`
- **Import name**: `chimera` (unchanged)

---

## Migration Checklist

- [x] Create `pyproject.toml` with all dependencies
- [x] Add missing HySDS sibling dependencies
- [x] Preserve all other dependency pins exactly
- [x] Update `chimera/__init__.py` for dynamic versioning
- [x] Add GitHub Actions workflow for PyPI publishing
- [x] Keep minimal `setup.py` shim for backward compatibility
- [x] Verify `python -m build` succeeds
- [ ] Tag v7.0.0 release
- [ ] Configure PyPI Trusted Publisher
- [ ] Publish to PyPI

---

## Contact

For questions about this migration, contact the HySDS team at hysds-help@jpl.nasa.gov
