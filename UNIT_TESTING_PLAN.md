# Unit Testing Plan for Chimera Python 3.12 Migration

## Analysis Summary

### Current Test Issues
- **Environment Dependencies**: Tests require specific local dev setups with hardcoded paths
- **External Service Dependencies**: Tests need SSH tunnels to Elasticsearch clusters
- **Missing Modules**: Tests import non-existent modules (`smap_sciflo`, `swot_chimera`)
- **Manual Setup Required**: Tests need manual configuration changes in multiple files

### Testable Core Components Identified
1. **Configuration Management** (`chimera/commons/conf_util.py`)
2. **Input Preprocessing** (`chimera/input_preprocessor.py`) 
3. **Job Submission Logic** (`chimera/run_pge_docker.py`)
4. **Post Processing** (`chimera/post_processor.py`)
5. **Utility Functions** (`chimera/commons/sciflo_util.py`)

## Unit Testing Strategy

### Phase 1: Configuration & Utility Tests
**Target**: `chimera/commons/conf_util.py`, `chimera/commons/constants.py`

**Test Files to Create**:
- `tests/unit/test_conf_util.py` - YamlConf, load_config functions
- `tests/unit/test_constants.py` - ChimeraConstants validation

**Key Test Cases**:
- YamlConf initialization with valid/invalid YAML
- Configuration loading and key access
- Error handling for missing config files
- JobContext and DockerParams object creation

### Phase 2: Core Workflow Tests  
**Target**: `chimera/input_preprocessor.py`, `chimera/run_pge_docker.py`, `chimera/post_processor.py`

**Test Files to Create**:
- `tests/unit/test_input_preprocessor.py` - process() function isolation
- `tests/unit/test_run_pge_docker.py` - submit_pge_job() function isolation  
- `tests/unit/test_post_processor.py` - post_process() function isolation

**Key Test Cases**:
- Input validation and error handling
- Configuration loading and validation
- Module loading and class instantiation logic
- Return value structures and types

### Phase 3: Component Integration Tests
**Target**: `chimera/precondition_evaluator.py`, `chimera/postprocess_evaluator.py`

**Test Files to Create**:
- `tests/unit/test_precondition_evaluator.py` - PreConditionEvaluator class
- `tests/unit/test_postprocess_evaluator.py` - PostProcessor class

**Key Test Cases**:
- Constructor parameter validation
- evaluate() method with mock data
- Configuration-driven behavior testing
- Error propagation and handling

### Phase 4: SciFlo Utility Tests
**Target**: `chimera/commons/sciflo_util.py`

**Test Files to Create**:
- `tests/unit/test_sciflo_util.py` - SciFlo utilities

**Key Test Cases**:
- Error extraction and file handling
- Placeholder file creation/cleanup
- Work directory operations (mocked)

## Mock Strategy

### External Dependencies to Mock
1. **File System Operations**: Use `unittest.mock.patch` for file I/O
2. **YAML Loading**: Mock PyYAML operations for config testing
3. **Module Imports**: Mock `import_module()` for dynamic loading tests
4. **SciFlo External Calls**: Mock `os.system()` calls to sflExec.py
5. **Elasticsearch**: Mock any ES client operations

### Test Data Strategy
**Create**: `tests/fixtures/` directory with:
- `sample_chimera_config.yaml` - Valid chimera configuration
- `sample_pge_config.json` - Valid PGE configuration
- `sample_sf_context.json` - Valid SciFlo context
- `sample_settings.yaml` - Valid settings file

### Isolation Techniques
- **Dependency Injection**: Modify functions to accept mock dependencies
- **Monkeypatching**: Use pytest.monkeypatch for external calls
- **Temporary Files**: Use `tempfile` for file system tests
- **Environment Variables**: Mock environment-dependent behavior

## Test Infrastructure Setup

### Directory Structure
```
tests/
├── __init__.py
├── fixtures/
│   ├── sample_chimera_config.yaml
│   ├── sample_pge_config.json  
│   ├── sample_sf_context.json
│   └── sample_settings.yaml
├── unit/
│   ├── __init__.py
│   ├── test_conf_util.py
│   ├── test_input_preprocessor.py
│   ├── test_run_pge_docker.py
│   ├── test_post_processor.py
│   ├── test_precondition_evaluator.py
│   ├── test_postprocess_evaluator.py
│   └── test_sciflo_util.py
└── conftest.py  # pytest configuration and shared fixtures
```

### Required Test Dependencies
```python
# Add to dev-requirements.txt
pytest>=7.0.0
pytest-mock>=3.10.0  
pytest-cov>=4.0.0    # Coverage reporting
pytest-xdist>=3.0.0  # Parallel test execution
```

### pytest Configuration (`conftest.py`)
- Shared fixtures for mock configurations
- Test data loading utilities
- Common mock setups for external dependencies

## Success Criteria

### Coverage Targets
- **Unit Test Coverage**: 80%+ for core functions
- **Critical Path Coverage**: 95%+ for main workflow functions
- **Error Handling Coverage**: 100% for exception paths

### Validation Approach
1. **Baseline Testing**: Run new tests with current Python setup
2. **Python 3.12 Testing**: Run same tests post-migration
3. **Regression Detection**: Compare test outputs between versions
4. **Performance Benchmarking**: Ensure no significant performance degradation

### Integration with Migration Plan
- **Phase 0**: Create and validate tests with current system
- **Phase 1-3**: Run tests during dependency/code modernization
- **Phase 4**: Use tests for regression validation post-upgrade

This testing strategy provides isolated, reproducible tests that don't rely on external services or specific local environments, enabling reliable validation of the Python 3.12 migration.