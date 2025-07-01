#!/bin/bash
# Docker-based code formatting script for Chimera Python 3.12
# Usage: ./format.sh

set -e

echo "Running code formatting in Python 3.12 Docker container..."

docker run --rm \
  -v "$(pwd):/app" \
  -w /app \
  python:3.12 bash -c "
    echo 'Installing dependencies...'
    pip install -r dev-requirements.txt --quiet
    
    echo 'Running pyupgrade for Python 3.12+...'
    find chimera tests -name '*.py' -exec pyupgrade --py312-plus {} \;
    
    echo 'Running black formatter...'
    black chimera/ tests/
    
    echo 'Running isort for import ordering...'
    isort chimera/ tests/
    
    echo 'Code formatting completed!'
  "