#!/bin/bash
# Script to run Phase 1 unit tests using Docker Python 3.12

echo "Running Phase 1 Unit Tests (Configuration & Utility Tests)"
echo "==========================================================="

docker run --rm -v $(pwd):/app -w /app python:3.12 bash -c "
  echo 'Installing dependencies...'
  pip install -e . >/dev/null 2>&1
  pip install PyYAML >/dev/null 2>&1
  pip install -r dev-requirements.txt >/dev/null 2>&1
  
  echo 'Running tests with coverage...'
  python -m pytest tests/unit/test_conf_util.py tests/unit/test_constants.py \
    --cov=chimera.commons.conf_util \
    --cov=chimera.commons.constants \
    --cov-report=term-missing \
    -v
"

echo ""
echo "Phase 1 testing completed!"