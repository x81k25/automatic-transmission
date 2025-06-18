# automatic transmission

A personal media automation system that ingests media item files via RSS feeds, downloads them through transmission-daemon, and organizes the content into a proper media library structure compatible with Plex and other media server applications.

## overview

Automatic transmission provides an intelligent, end-to-end pipeline for downloading and organizing media content with minimal manual intervention:

**core pipeline:**
- **rss ingestion** - monitors multiple RSS feeds (YTS, episodefeed) for new content
- **collection & parsing** - processes existing downloads and extracts metadata from filenames
- **file filtration** - filters content based on technical specs (resolution, codecs, etc.)
- **metadata enrichment** - collects rich metadata from TMDB and OMDb APIs
- **ML-powered filtering** - uses reel-driver API for intelligent recommendation scoring
- **download management** - initiates, monitors, and handles failed downloads
- **library organization** - transfers and organizes content for Plex compatibility
- **automated cleanup** - maintains system health and removes completed torrents

**supported media types:**
- movies (YTS source)
- TV shows - individual episodes (episodefeed source)  
- TV seasons - complete seasons (episodefeed source)

**key features:** 
- ML-powered filtering 
- automated metadata collection 
- multi-source RSS support 
- comprehensive error handling

## prerequisites

- Python 3.12
- PostgreSQL database
- Transmission daemon running on the local network
- API credentials for metadata services (TMDB, OMDb)
- connection to the reel-driver API for media recommendation scoring

## repository structure

```
automatic-transmission/
├── config/                   # Configuration files
│   ├── filter-parameters.yaml
│   └── string-special-conditions.yaml
├── src/                      # source code
│   ├── core/                 # core pipeline modules
│   │   ├── __init__.py
│   │   ├── _01_rss_ingest.py
│   │   ├── _02_collect.py
│   │   ├── _03_parse.py
│   │   ├── _04_file_filtration.py
│   │   ├── _05_metadata_collection.py
│   │   ├── _06_media_filtration.py
│   │   ├── _07_initiation.py
│   │   ├── _08_download_check.py
│   │   ├── _09_transfer.py
│   │   └── _10_cleanup.py
│   ├── data_models/          # data models for validation/transformation
│   │   ├── __init__.py
│   │   └── media_data_frame.py
│   ├── error_handling/       # ad-hoc pipeline maintenance  
│   │   └── error_handling.py
│   └── utils/                # utility modules
│       ├── __init__.py
│       ├── local_file_operations.py
│       ├── log_config.py
│       ├── parse_element.py
│       ├── rpcf.py
│       └── sqlf.py
├── tests/                    # test scripts and resources
│   ├── unit/                 # unit tests
│   ├── fixtures/             # test fixtures
│   ├── config/               # test configuration files
│   └── conftest.py
├── .github/workflows/        # GitHub Actions CI/CD
├── .venv/                    # virtual environment (gitignored)
├── .env                      # environment variables (gitignored)
├── .gitignore
├── .python-version           # python version specification
├── changelog.md              # contains all major/minor version changes
├── main.py                   # main execution script
├── pytest.ini                # pytest configuration
├── readme.md                 # this file
├── requirements.in           # direct dependencies
└── requirements.txt          # pinned dependencies
```

## core data model

The project centers around the `MediaDataFrame` class, which serves as a rigid schema-enforcing wrapper around a polars DataFrame. This class:

- mirrors the structure of the consolidated PostgreSQL `media` table
- performs input validation before database operations
- handles timestamp conversion and type enforcement
- standardizes data operations throughout the pipeline

## installation

1. clone this repository:
   ```bash
   git clone https://github.com/yourusername/automatic-transmission.git
   cd automatic-transmission
   ```

2. create a virtual environment and activate it:
   ```bash
   python -m venv .venv
   # on Windows
   .venv\Scripts\activate
   # on Unix or MacOS
   source .venv/bin/activate
   ```

3. install dependencies using uv (recommended):
   ```bash
   # install uv if you don't have it
   pip install uv
   
   # install dependencies from requirements.txt
   uv pip install -r requirements.txt
   ```

   alternatively, you can use pip:
   ```bash
   pip install -r requirements.txt
   ```

4. set up the PostgreSQL database objects:
   - See the `schema-owners-manual` project for documentation covering database schema creation and setup
   - *Note: URL will be added when available*

5. configure your environment variables by creating a `.env` file with the required parameters (see Configuration section below)

6. optional: Set up connection to the reel-driver API if you want to use ML-based media recommendation scoring

### adding new packages:
```bash
# add new package to requirements.in
echo "new-package" >> requirements.in

# compile requirements.in to requirements.txt
uv pip compile requirements.in -o requirements.txt

# install the updated requirements
uv pip install -r requirements.txt
```

## docker deployment

The project includes Docker support for containerized deployment. All Docker configuration and detailed instructions are available in `containerization/readme.md`.

**Quick start with Docker:**
```bash
# Build all images
docker-compose -f containerization/docker-compose.yml build

# Run specific pipeline modules
docker-compose -f containerization/docker-compose.yml run --rm rss-ingest
docker-compose -f containerization/docker-compose.yml run --rm collect
```

**Key features:**
- Multi-stage Dockerfiles for optimized builds
- Environment variable support for all configuration
- Local development and CI/CD compatible
- Images published to GitHub Container Registry

See `containerization/readme.md` for comprehensive Docker setup, usage examples, and deployment guidance.

## configuration

create a `.env` file with the following parameters customized for your setup:

### pipeline control parameters

```bash
# Batch processing
BATCH_SIZE=50                    # Controls batch sizes for scripts with external dependencies
LOG_LEVEL=DEBUG                  # Sets log level (DEBUG, INFO, WARNING, ERROR)

# Metadata collection
STALE_METADATA_THRESHOLD=30      # Days after which metadata is considered stale and recollected

# Media filtration  
REEL_DRIVER_THRESHOLD=0.35       # Minimum prediction probability for acceptance

# Cleanup automation
TARGET_ACTIVE_ITEMS=10           # Soft cap on download items (modulates cleanup timing)
TRANSFERRED_ITEM_CLEANUP_DELAY=0 # Days to wait before removing completed items (0 = immediate)
HUNG_ITEM_CLEANUP_DELAY=1        # Days to wait before removing stalled downloads
```

### service credentials

```bash
# Transmission daemon
SERVER_IP=your_server_ip
TRANSMISSION_USERNAME=your_username
TRANSMISSION_PASSWORD=your_password
TRANSMISSION_PORT=9091

# PostgreSQL database
PG_ENDPOINT=your_postgres_host
PG_PORT=5432
PG_DATABASE=your_database_name
PG_USERNAME=your_postgres_username
PG_PASSWORD=your_postgres_password
PG_SCHEMA=your_schema_name
```

### API keys

```bash
# TMDB API (for movie metadata)
MOVIE_SEARCH_API_BASE_URL=https://api.themoviedb.org/3/search/movie
MOVIE_DETAILS_API_BASE_URL=https://api.themoviedb.org/3/movie
MOVIE_SEARCH_API_KEY=your_tmdb_api_key
MOVIE_DETAILS_API_KEY=your_tmdb_api_key

# TMDB API (for TV metadata)  
TV_SEARCH_API_BASE_URL=https://api.themoviedb.org/3/search/tv
TV_DETAILS_API_BASE_URL=https://api.themoviedb.org/3/tv
TV_SEARCH_API_KEY=your_tmdb_api_key
TV_DETAILS_API_KEY=your_tmdb_api_key

# OMDb API (for ratings)
MOVIE_RATINGS_API_BASE_URL=https://www.omdbapi.com/
MOVIE_RATINGS_API_KEY=your_omdb_api_key
TV_RATINGS_API_BASE_URL=https://www.omdbapi.com/
TV_RATINGS_API_KEY=your_omdb_api_key

# Reel-driver API (optional ML scoring)
REEL_DRIVER_HOST=your_reel_driver_host
REEL_DRIVER_PORT=your_reel_driver_port
REEL_DRIVER_PREFIX=api_prefix
```

### RSS feeds

```bash
# RSS feed configuration (sources and URLs must be in same order)
RSS_SOURCES=yts.mx,episodefeed.com
RSS_URLS=your_movie_rss_url,your_tv_show_rss_url
```
> **RSS Setup:** Generate feeds at [YTS](https://yts.torrentbay.st/rss-guide) for movies and [episodefeed](https://episodefeed.com/) for TV shows

### directory paths

```bash
# File system paths
DOWNLOAD_DIR=/path/to/transmission/downloads
MOVIE_DIR=/path/to/movie/library
TV_SHOW_DIR=/path/to/tv/library
```

### configuration files

additional configuration is available in:
- `config/filter-parameters.yaml`: media filtering parameters (resolution, codecs, etc.)
- `config/string-special-conditions.yaml`: special string handling for edge case filenames

## usage

Execute the complete pipeline using the command line interface:

```bash
python main.py
```

### debug mode
Add the `--debug` flag to enable verbose logging:
```bash
python main.py --debug
```

### first run setup
1. Ensure transmission-daemon is running and accessible
2. Verify database connection and schema setup
3. Test API credentials with a small batch
4. Review filter parameters in `config/filter-parameters.yaml`
5. Monitor logs for any configuration issues

### monitoring
- Check logs for pipeline progress and errors
- Monitor transmission daemon for download status
- Verify media organization in target directories
- Review database for pipeline status tracking

### common workflows
```bash
# Standard daily run
python main.py

# Debug run with verbose output
python main.py --debug

# Check current pipeline status (via database)
# Monitor active downloads (via transmission web interface)
# Review recent logs for issues
```

## testing

Run the test suite using pytest:

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test modules
pytest tests/unit/core/
pytest tests/unit/utils/
```

### test coverage

**core pipeline modules:**
- `_02_collect.py` - 67%
- `_03_parse.py` - 75%
- `_04_file_filtration.py` - 67%
- `_05_metadata_collection.py` - 50%
- `_06_media_filtration.py` - 80%
- `_07_initiation.py` - 33%
- `_08_download_check.py` - 75%
- `_09_transfer.py` - 50%

**utility modules:**
- `local_file_operations.py` - 86%
- `parse_element.py` - 91%

### continuous integration

Tests run automatically via GitHub Actions:
- **push to `dev` branch** - triggers full test suite
- **pull requests to `stg` or `main`** - triggers full test suite
- environment-specific variables loaded based on target branch

**test configuration:**
- fixtures organized by module in `tests/fixtures/`
- comprehensive test scenarios covering edge cases
- pytest configuration in `pytest.ini`

## documentation

Additional documentation is available in the project's wiki. Refer to the wiki for more detailed information about the pipeline processes, troubleshooting, and advanced configurations.

## license

This project is licensed under the MIT License.