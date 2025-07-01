# Python 3.12 Upgrade Plan for Chimera

## Analysis Summary
- **Current State**: No explicit Python version constraints, mixed dependency versions between setup.py and requirements.txt
- **Dependencies**: Elasticsearch 6.2.0/7.x conflict, outdated packages (setuptools 38.2.5, requests 2.20.0)  
- **Code Quality**: Uses old-style string formatting (%s), but no major Python 2/3 compatibility issues found
- **Infrastructure**: No existing Docker setup, no CI/CD pipeline

## Key Decisions
- **Python 3.12 Only**: Target Python 3.12 exclusively, remove backward compatibility
- **Keep Elasticsearch 7.x**: Maintain compatibility with existing HySDS infrastructure (avoid 8.x breaking changes)
- **Docker as Remote Interpreter**: Use `docker run -it --rm -v $(pwd):/app -w /app python:3.12 bash` for all development

## Phase 0: Baseline Testing (Pre-Migration)
1. **Current System Validation**: Test existing codebase using Docker with current Python to establish baseline
2. **Document Current Behavior**: Run existing tests and document expected outputs using consistent Docker environment
3. **Identify Test Gaps**: Determine areas needing additional test coverage before migration
4. **Create Test Data Snapshots**: Capture current test results for comparison post-migration

### Phase 0 Implementation - Docker-based Baseline
```bash
# Determine current Python version for baseline testing
CURRENT_PYTHON=3.12
echo "Using Python $CURRENT_PYTHON for baseline testing"

# Test current system using Docker with current Python version
docker run -it --rm -v $(pwd):/app -w /app python:$CURRENT_PYTHON bash -c "
  pip install -e . 2>/dev/null || echo 'Install failed, trying with current requirements'
  pip install -r chimera/requirements.txt 2>/dev/null || echo 'Requirements install failed'
  python -m pytest tests/ -v --tb=short" > baseline_test_results.txt 2>&1

# Document current environment in Docker
docker run -it --rm -v $(pwd):/app -w /app python:$CURRENT_PYTHON bash -c "
  python --version
  pip install -e . 2>/dev/null || pip install -r chimera/requirements.txt 2>/dev/null || true
  pip freeze" > current_environment.txt 2>&1

# Run existing test scripts using Docker
docker run -it --rm -v $(pwd):/app -w /app python:$CURRENT_PYTHON bash -c "
  pip install -e . 2>/dev/null || pip install -r chimera/requirements.txt 2>/dev/null || true
  cd tests/
  python test_input_preprocessor.py" > baseline_input_preprocessor.txt 2>&1

docker run -it --rm -v $(pwd):/app -w /app python:$CURRENT_PYTHON bash -c "
  pip install -e . 2>/dev/null || pip install -r chimera/requirements.txt 2>/dev/null || true
  cd tests/
  python test_post_processor.py" > baseline_post_processor.txt 2>&1

docker run -it --rm -v $(pwd):/app -w /app python:$CURRENT_PYTHON bash -c "
  pip install -e . 2>/dev/null || pip install -r chimera/requirements.txt 2>/dev/null || true
  cd tests/
  python test_run_pge_docker.py" > baseline_run_pge_docker.txt 2>&1

# Validate current flake8 compliance using Docker
docker run -it --rm -v $(pwd):/app -w /app python:$CURRENT_PYTHON bash -c "
  pip install flake8 2>/dev/null || true
  flake8 chimera/" > baseline_flake8.txt 2>&1 || true

# Create baseline archive
tar -czf baseline_results.tar.gz baseline_*.txt current_environment.txt
```

## Phase 1: Dependency Management & Compatibility
1. **Consolidate Dependencies**: Remove chimera/requirements.txt, use only setup.py for dependency management
2. **Update setup.py**: Add `python_requires=">=3.12"` and upgrade dependencies:
   - elasticsearch: 7.10.0+ (latest 7.x with Python 3.12 support)
   - elasticsearch-dsl: 7.4.0+ (latest 7.x compatible)
   - requests: >=2.28.0 (Python 3.12 support)
   - simplejson: >=3.18.0 (Python 3.12 support)
   - PyYAML: >=6.0 (replaces 'yaml' package, Python 3.12 support)
3. **Add Development Dependencies**: Create `dev-requirements.txt` with testing/linting tools

## Phase 2: Docker Remote Interpreter Setup ✅ COMPLETED
1. **Docker Development Workflow**: Use `python:3.12` container directly as remote interpreter ✅
2. **Create Testing Scripts**: Shell scripts for running tests, linting, and formatting in Docker ✅
3. **Development Commands**: ✅
   ```bash
   # Interactive development session
   docker run -it --rm -v $(pwd):/app -w /app python:3.12 bash
   
   # Install dependencies in container
   pip install -e .
   pip install -r dev-requirements.txt
   ```
4. **Update CLAUDE.md**: Document new Docker-based development workflow ✅

### Phase 2 Implementation Details
**Completed:** All objectives successfully implemented and tested.

**Key Achievements:**
- **Fixed setup.py Dependencies**: Updated with Python 3.12 compatible versions including missing PyYAML
- **Created Development Scripts**: 
  - `test.sh` - Docker-based pytest execution
  - `lint.sh` - flake8, black, and isort checking
  - `format.sh` - pyupgrade, black, and isort formatting
- **Verified Functionality**: 30 unit tests pass successfully in Python 3.12 environment
- **Code Quality Improvement**: Reduced flake8 issues from 30+ to 13 through automated formatting
- **Documentation Updated**: CLAUDE.md now includes Docker workflow commands

**Testing Results:**
- All 30 unit tests (conf_util, input_preprocessor) pass in Python 3.12
- Code formatting with black and isort successful
- Pyupgrade applied Python 3.12+ modernizations
- Remaining flake8 issues are minor (unused variables, whitespace)

## Phase 3: Code Modernization & Quality Tools  
1. **Install pyupgrade**: Add to dev dependencies, configure for Python 3.12+ target (`--py312-plus`)
2. **Add Code Formatters**: black, isort for consistent formatting
3. **Enhance Linting**: Add pylint, mypy type checking alongside existing flake8
4. **String Formatting**: Use pyupgrade to modernize %s formatting to f-strings where appropriate
5. **Create pyproject.toml**: Modern Python project configuration
6. **Manual Code Refactoring**:
   - **File Handling**: Systematically update all file I/O to use `with open(...)` to prevent resource leaks.
   - **Exception Handling**: Replace bare `except:` clauses with specific exceptions and remove redundant `raise` statements.
   - **Logging**: Convert debugging `print()` statements to `logger` calls.

## Phase 4: Testing & Validation
1. **Modernize Test Suite**: Refactor existing test runner scripts (e.g., `tests/test_post_processor.py`) into a structured test suite using `pytest`. This includes converting test logic into test functions and using fixtures for setup/teardown.
2. **Docker-based Testing**: Create shell scripts for running tests in Python 3.12 containers:
   ```bash
   # test.sh
   docker run -it --rm -v $(pwd):/app -w /app python:3.12 bash -c "
     pip install -e . && pip install -r dev-requirements.txt && pytest tests/ -v"
   ```
3. **Regression Testing**: Compare Python 3.12 test results against baseline from Phase 0:
   ```bash
   # Run Python 3.12 tests and compare with baseline
   docker run -it --rm -v $(pwd):/app -w /app python:3.12 bash -c "
     pip install -e . && pip install -r dev-requirements.txt && pytest tests/ -v --tb=short" > python312_test_results.txt 2>&1
   
   # Compare test outputs
   diff baseline_test_results.txt python312_test_results.txt || echo "Differences found - review required"
   
   # Validate individual test behavior consistency using Docker
   for test_script in test_input_preprocessor.py test_post_processor.py test_run_pge_docker.py; do
     docker run -it --rm -v $(pwd):/app -w /app python:3.12 bash -c "
       pip install -e . && pip install -r dev-requirements.txt
       cd tests/ && python $test_script" > python312_${test_script%.py}.txt 2>&1
     diff baseline_${test_script%.py}.txt python312_${test_script%.py}.txt || echo "Differences in $test_script - review required"
   done
   ```
4. **CI Setup**: GitHub Actions workflow using `python:3.12` container for automated testing
5. **Integration Testing**: Ensure HySDS/SciFlo compatibility is maintained
6. **Documentation Update**: Update installation instructions and development setup

## Key Files to Create/Modify
- `test.sh`, `lint.sh`, `format.sh` - Docker-based development scripts
- `pyproject.toml` - Modern project config with tool settings
- `dev-requirements.txt` - Development dependencies
- `setup.py` - Updated dependencies and Python version constraints
- `.github/workflows/test.yml` - CI pipeline using Python 3.12 container
- `CLAUDE.md` - Updated with Docker workflow commands
- Remove: `chimera/requirements.txt` (consolidate to setup.py)

## Migration Strategy
- **Python 3.12 Only**: No backward compatibility, exclusive targeting of Python 3.12
- **Pin dependency versions**: Ensure reproducible builds with minimum Python 3.12 compatible versions
- **Gradual migration approach**: Validate each phase before proceeding
- **Keep existing API interfaces unchanged**: Maintain HySDS/SciFlo integration compatibility

## Detailed Implementation Steps

### Phase 0: Baseline Testing (Docker-based)
```bash
# 1. Determine current Python version for consistent baseline testing
CURRENT_PYTHON=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
echo "Using Python $CURRENT_PYTHON for baseline testing"

# 2. Test current system using Docker with current Python version
docker run -it --rm -v $(pwd):/app -w /app python:$CURRENT_PYTHON bash -c "
  pip install -e . 2>/dev/null || pip install -r chimera/requirements.txt 2>/dev/null || true
  python -m pytest tests/ -v --tb=short" > baseline_test_results.txt 2>&1

# 3. Document current environment in Docker
docker run -it --rm -v $(pwd):/app -w /app python:$CURRENT_PYTHON bash -c "
  python --version
  pip install -e . 2>/dev/null || pip install -r chimera/requirements.txt 2>/dev/null || true
  pip freeze" > current_environment.txt 2>&1

# 4. Run individual test scripts using Docker
for test_script in test_input_preprocessor.py test_post_processor.py test_run_pge_docker.py; do
  docker run -it --rm -v $(pwd):/app -w /app python:$CURRENT_PYTHON bash -c "
    pip install -e . 2>/dev/null || pip install -r chimera/requirements.txt 2>/dev/null || true
    cd tests/ && python $test_script" > baseline_${test_script%.py}.txt 2>&1
done

# 5. Check current code quality using Docker
docker run -it --rm -v $(pwd):/app -w /app python:$CURRENT_PYTHON bash -c "
  pip install flake8 2>/dev/null || true
  flake8 chimera/" > baseline_flake8.txt 2>&1 || true

# 6. Create baseline archive
tar -czf baseline_results.tar.gz baseline_*.txt current_environment.txt
echo "Baseline testing completed. Results archived in baseline_results.tar.gz"
```

### Phase 1: Dependency Management
```bash
# 1. Remove conflicting requirements.txt
rm chimera/requirements.txt

# 2. Update setup.py dependencies
# - Add python_requires=">=3.12"
# - Update elasticsearch to 7.10.0+ (Python 3.12 compatible, stay in 7.x)
# - Update requests to >=2.28.0 (Python 3.12 support)
# - Add PyYAML>=6.0, simplejson>=3.18.0
```

### Phase 2: Docker Remote Interpreter Setup
```bash
# Interactive development session
docker run -it --rm -v $(pwd):/app -w /app python:3.12 bash

# Create development scripts
# test.sh
docker run -it --rm -v $(pwd):/app -w /app python:3.12 bash -c "
  pip install -e . && pip install -r dev-requirements.txt && pytest tests/ -v"

# lint.sh  
docker run -it --rm -v $(pwd):/app -w /app python:3.12 bash -c "
  pip install -r dev-requirements.txt && flake8 chimera/ && black --check chimera/ tests/"

# format.sh
docker run -it --rm -v $(pwd):/app -w /app python:3.12 bash -c "
  pip install -r dev-requirements.txt && pyupgrade --py312-plus \$(find chimera tests -name '*.py') && black chimera/ tests/ && isort chimera/ tests/"
```

### Phase 3: Modernization Tools
```bash
# Install development tools
pip install pyupgrade black isort pylint mypy pytest

# Run pyupgrade on codebase
find . -name "*.py" -exec pyupgrade --py312-plus {} \;

# Format code
black chimera/ tests/
isort chimera/ tests/
```

### Phase 4: Testing Strategy
```yaml
# .github/workflows/test.yml
name: Test Python 3.12
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    container: python:3.12
    steps:
      - uses: actions/checkout@v4
      - run: pip install -e .
      - run: pip install -r dev-requirements.txt  
      - run: pytest tests/
      - run: flake8 chimera/
      - run: black --check chimera/ tests/
      - run: isort --check-only chimera/ tests/
```

## Dependencies Analysis

### Current Conflicts
- **setup.py**: elasticsearch>=7.0.0,<7.14.0
- **requirements.txt**: elasticsearch==6.2.0
- **requirements.txt**: Fixed old versions (setuptools==38.2.5, requests==2.20.0)

### Proposed Resolution - Python 3.12 Compatible
```python
# setup.py
python_requires=">=3.12"
install_requires=[
    'elasticsearch>=7.10.0,<8.0.0',  # Latest 7.x with Python 3.12 support
    'elasticsearch-dsl>=7.4.0,<8.0.0',  # Latest 7.x compatible
    'requests>=2.28.0',  # Python 3.12 support
    'simplejson>=3.18.0',  # Python 3.12 support
    'PyYAML>=6.0',  # Replace yaml package, Python 3.12 support
]
```

## Risk Mitigation
1. **Baseline Testing**: Capture current system behavior before any changes (Phase 0)
2. **Gradual Rollout**: Test each phase in isolation with regression testing
3. **Version Pinning**: Maintain specific version ranges for stability
4. **Fallback Plan**: Keep baseline results and current setup.py as backup
5. **Integration Testing**: Validate with existing HySDS workflows using baseline comparison
6. **Documentation**: Clear migration guide with rollback procedures