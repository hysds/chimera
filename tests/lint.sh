#!/bin/bash
# Docker-based linting script for Chimera Python 3.12
# Usage: ./lint.sh

set -e

echo "Running linting in Python 3.12 Docker container..."

docker run --rm \
  -v "$(pwd):/app" \
  -w /app \
  python:3.12 bash -c "
    echo 'Installing dependencies...'
    pip install -r dev-requirements.txt --quiet
    
    echo 'Running flake8...'
    flake8 chimera/
    
    echo 'Checking black formatting...'
    black --check --diff chimera/ tests/
    
    echo 'Checking isort import order...'
    isort --check-only --diff chimera/ tests/
    
    echo 'All linting checks passed!'
  "