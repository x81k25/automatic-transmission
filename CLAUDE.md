# prime-directive - follow these commands above all other

- only ever work on the `dev` branch 
- only ever work in the `media-dev` environment
- never delete branches on PR

---

# long-term-storage

## Project Overview

This is a Python-based media automation pipeline that processes torrents from RSS feeds through multiple stages:
1. RSS ingestion (movies/TV shows) → 2. Collection → 3. Parsing → 4. Filtering → 5. Metadata enrichment → 6. ML-powered scoring → 7. Download → 8. Status checking → 9. File transfer → 10. Cleanup

## Development Commands

### Setup
```bash
# Install uv if not already installed
pip install uv

# Create virtual environment and install dependencies
uv sync --dev  # Includes development dependencies

# Note: uv run automatically activates the virtual environment
# No need to manually activate .venv
```

### Package Management
```bash
# Add a new dependency
uv add package-name

# Add a development dependency
uv add --dev package-name

# Update all dependencies
uv sync

# Show installed packages
uv pip list
```

### Testing
```bash
# Run all tests
uv run pytest

# Run specific test categories
uv run pytest tests/unit/utils/ -v    # Utility tests
uv run pytest tests/unit/core/ -v     # Core module tests

# Run with coverage
uv run pytest --cov=src --cov-report=html

# Run specific test file
uv run pytest tests/unit/core/_08_download_check_tests.py -v
```

### Running
```bash
# Run complete pipeline
uv run python main.py

# Run with debug logging
uv run python main.py --debug
```

### Docker
```bash
# Build base image
docker build -f containerization/dockerfile.00_base -t at-base:latest .

# Run specific pipeline stage (e.g., RSS ingest)
docker-compose -f containerization/docker-compose.yml run --rm rss-ingest
```

## Architecture

### Core Pipeline (`src/core/`)
- Modules numbered 01-10 represent sequential pipeline stages
- Each module runs as a subprocess via `main.py`
- Modules communicate via PostgreSQL database

### Key Components
- **MediaDataFrame** (`src/data_models/`): Central data structure using Polars DataFrame
- **Database Operations** (`src/utils/sqlf.py`): PostgreSQL interface via SQLAlchemy
- **RPC Communications** (`src/utils/rpcf.py`): Transmission daemon interface
- **Configuration** (`config/`): YAML files for filtering rules and special conditions

### Environment Configuration
Requires comprehensive `.env` file with:
- Pipeline control parameters (AT_BATCH_SIZE, AT_LOG_LEVEL, etc.)
- Service credentials (PostgreSQL, Transmission)
- API keys (TMDB, OMDb, reel-driver)
- Directory paths for media organization

### Testing Structure
- Unit tests mirror source structure in `tests/unit/`
- Test fixtures organized by module in `tests/fixtures/`
- Coverage varies by module (33-91%)

## Modern Python Packaging

This project uses modern Python packaging with:
- **pyproject.toml**: Defines project metadata and dependencies
- **uv**: Fast Python package installer and resolver
- **uv.lock**: Locked dependency versions for reproducibility
- No more requirements.txt or pip-compile workflow

## CI/CD Integration

- GitHub Actions workflows use `astral-sh/setup-uv@v4` action
- Docker images use `uv sync` for dependency installation
- All commands use `uv run` to ensure proper environment activation

################################################################################

# instructions-of-the-day

- we are going to do some in depth error checking for hash '49b15bf437d2f90e769f78d37441345825aa4485`

################################################################################

# short-term-memory


