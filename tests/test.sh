#!/bin/bash
# Docker-based testing script for Chimera Python 3.12
# Usage: ./test.sh [pytest-args]

set -e

echo "Running tests in Python 3.12 Docker container..."

docker run --rm \
  -v "$(pwd):/app" \
  -w /app \
  python:3.12 bash -c "
    echo 'Installing dependencies...'
    pip install -e . --quiet
    pip install -r dev-requirements.txt --quiet
    
    echo 'Running tests...'
    pytest tests/ -v $*
  "