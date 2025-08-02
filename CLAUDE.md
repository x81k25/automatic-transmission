# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Last updated: 2025-08-02

## prime-directive - follow these commands above all other

- only ever work on the `dev` branch 
- only ever work in the `media-dev` environment
- never delete branches on PR

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

- if media is None and current_media_items is None
  - there is nothing to do

- if media is None and current_media_itmes is not None
  - all of the current_media_items should be re-ingested

- if media is not None and current_media_items is None
  - media items that are also download complete should be taggas as complete
  - but there are not current_meida items to re-ingest

- if meida is not None and current_media_items is not None
  - some items will be re-ingested, some items may or may not be tagged as download compelte depending on status


################################################################################

# your-section - include all notes, issues, comments, and progress here

## AttributeError Diagnosis (_08_download_check.py)

**Error:** `AttributeError: 'NoneType' object has no attribute 'df'`

**Root Cause Analysis:**
The error occurs in `src/core/_08_download_check.py:24` in the `confirm_downloading_status` function:

```python
def confirm_downloading_status(media: MediaDataFrame, current_media_items: dict | None) -> MediaDataFrame:
    media_not_downloading = media.df.clone()  # <-- LINE 24: FAILS HERE
```

**Issue Flow:**
1. `check_downloads()` function calls `utils.get_media_from_db(pipeline_status='downloading')` at line 161
2. This returns `None` when no media items have `pipeline_status='downloading'`  
3. The code checks `if media is None and current_media_items is None: return` at line 167
4. **BUG:** If `media is None` but `current_media_items is NOT None`, the condition fails
5. Execution continues to line 171-174 calling `confirm_downloading_status(media, current_media_items)`
6. Inside `confirm_downloading_status`, line 24 tries to access `media.df` where `media=None`
7. Results in `AttributeError: 'NoneType' object has no attribute 'df'`

**Specific Condition Triggering Error:**
- Database has NO items with `pipeline_status='downloading'` (`media = None`)
- BUT Transmission daemon HAS active torrents (`current_media_items != None`)
- This bypasses the early return condition and passes `None` to a function expecting a MediaDataFrame

**Location:** `src/core/_08_download_check.py:24` in `confirm_downloading_status()`

## External API References

- the real driver api docs can be found at http://192.168.50.2:30802/reel-driver/openapi.json

## Development Best Practices

- always use uv run to run python scripts and tests