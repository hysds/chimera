# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Chimera is a SciFlo-based workflow framework for HySDS that provides a standardized skeleton for running Product Generation Executable (PGE) workflows. It implements a 3-step pipeline: Input Preprocessing, PGE Execution, and Post Processing. The framework is designed to be easily adapted by any project within the HySDS ecosystem.

## Key Commands

### Docker Development Workflow (Python 3.12)
This project uses Docker containers for consistent Python 3.12 development environment.

#### Interactive Development Session
```bash
# Start interactive Python 3.12 container
docker run -it --rm -v $(pwd):/app -w /app python:3.12 bash

# Inside container - install dependencies
pip install -e .
pip install -r dev-requirements.txt
```

#### Testing
```bash
# Run all tests using Docker
./tests/test.sh

# Run specific test with additional pytest arguments
./tests/test.sh tests/test_input_preprocessor.py -v

# Traditional testing (if Docker unavailable)
python -m pytest tests/
```

#### Code Quality
```bash
# Run linting checks using Docker
./tests/lint.sh

# Format code using Docker
./tests/format.sh

# Traditional linting (if Docker unavailable)
flake8 chimera/
```

### Legacy Installation (Local Python)
```bash
pip install -e .
```

### Workflow Execution
```bash
# Run SciFlo workflow
python chimera/run_sciflo.py <sfl_file> <context_file> <output_folder>

# Execute via shell script
./chimera/run_sciflo.sh
```

## Architecture Overview

### Core Workflow Steps
1. **Input Preprocessor** (`input_preprocessor.py`): Evaluates preconditions and prepares input context for PGE execution
2. **PGE Job Submission** (`run_pge_docker.py`): Submits PGE jobs using configurable job submitter classes  
3. **Post Processing** (`post_processor.py`): Processes PGE results and creates context for subsequent workflow steps

### Key Components

#### Configuration System
- **Chimera Config** (`chimera/configs/chimera_config.yaml`): Main configuration defining module paths and class names for preprocessor and job_submitter components
- **PGE Configs** (`chimera/configs/pge_configs/`): Individual PGE-specific configuration files in JSON format
- **Settings Files**: Runtime settings passed to workflow steps

#### Core Classes
- **PreConditionEvaluator**: Evaluates input preconditions using configurable PreConditionFunctions
- **PgeJobSubmitter**: Base class for PGE job submission (must be subclassed for specific implementations)
- **PostProcessor**: Handles post-processing using configurable PostProcessFunctions
- **YamlConf**: Configuration file loader with JobContext and DockerParams support

#### SciFlo Integration
- **SciFlo Utilities** (`commons/sciflo_util.py`): Core SciFlo execution functions including `run_sciflo()`, error handling, and work directory management
- **Workflow Templates** (`wf_xml/`): SciFlo XML workflow definitions with examples for single/multiple PGE configurations

### Extension Points
The framework uses a plugin-style architecture where custom implementations are loaded via configuration:

1. **Preprocessor Functions**: Extend `PreConditionFunctions` class and configure in `chimera_config.yaml` 
2. **Job Submitters**: Extend `PgeJobSubmitter` class and configure in `chimera_config.yaml`
3. **Post-process Functions**: Extend `PostProcessFunctions` class

### Directory Structure
- `chimera/`: Main package with workflow step implementations
- `chimera/commons/`: Shared utilities (configuration, SciFlo integration, logging, constants)
- `chimera/configs/`: Configuration files and templates
- `chimera/wf_xml/`: SciFlo workflow XML definitions and examples
- `tests/`: Unit tests with test data files

### Dependencies
- Elasticsearch 7.x for job tracking and metadata storage
- SciFlo workflow engine (part of HySDS framework)
- Standard Python libraries for HTTP requests and JSON processing

## Code Style
- Maximum line length: 120 characters
- Uses flake8 with specific ignores for E501, W503, E722
- Follow existing patterns in the codebase for new components