# automatic transmission

A personal media automation system that ingests torrent files via RSS feeds, downloads them through transmission-daemon, and organizes the content into a proper media library structure compatible with Plex and other media server applications.

## overview

automatic transmission provides an end-to-end pipeline for downloading and organizing media content:

1. ingests torrent files from RSS feeds
2. downloads media through transmission-daemon
3. parses and categorizes the media files
4. collects metadata for organization
5. filters media based on configured parameters
6. initiates and monitors downloads
7. transfers completed downloads to appropriate library locations
8. performs cleanup operations

the system supports three media types:
- movies
- TV shows
- TV seasons

## prerequisites

- python 3.12
- PostgreSQL database
- Transmission daemon running on the local network

## repository structure:

```
automatic-transmission/
├── config/                   # Configuration files
│   ├── filter-parameters.json
│   └── string-special-conditions.yaml
├── sql/                      # SQL scripts for database setup
├── src/                      # source code
│   ├── core/                 # core pipeline modules
│   │   ├── __init__.py
│   │   ├── 01_rss_ingest.py
│   │   ├── 02_collect.py
│   │   ├── 03_parse.py
│   │   ├── 04_metadata_collection.py
│   │   ├── 05_filter.py
│   │   ├── 06_initiate.py
│   │   ├── 07_download_check.py
│   │   ├── 08_transfer.py
│   │   └── 09_cleanup.py
│   └── utils/                # utility modules
│       ├── __init__.py
│       ├── error_handling.py
│       ├── local_file_operations.py
│       ├── parse_element.py
│       ├── rpcf.py
│       └── sqlf.py
├── test/                     # test scripts and resources
├── venv/                     # virtual environment (gitignored)
├── .env                      # environment variables
├── .gitignore
├── changelog.md              # contains not all major/minor version changes
├── main.py                   # main execution script
├── pytest.ini
├── readme.md                 # this file
└── requirements.txt          # python dependencies
```

## installation

1. clone this repository:
   ```
   git clone https://github.com/yourusername/automatic-transmission.git
   cd automatic-transmission
   ```

2. install the required Python dependencies:
   ```
   pip install -r requirements.txt
   ```

3. set up the PostgreSQL database using the scripts in the `/sql` directory. The PostgreSQL scripts below contain placeholder values denoted by `<placeholder>` that will need to filled in with your specifications.
      
   ```
   psql -U your_username -d your_database -f sql/instantiate-schema.sql
   psql -U your_username -d your_database -f sql/back-up-create.sql
   psql -U your_username -d your_database -f sql/back-up-push.sql
   ```

4. configure your environment variables by copying and filling out the `.env` template:
   ```
   cp templates/.env .env
   ```

## configuration

A `.env` will need to created with all of the following parameters for your specific configuration. My configuration is currently set-up to with YTS for movies and episodefeed for TV.

```
# Transmission credentials
SERVER_IP='your_server_ip'
TRANSMISSION_USERNAME='your_transmission_username'
TRANSMISSION_PASSWORD='your_transmission_password'

# PostgreSQL credentials
PG_ENDPOINT='your_postgres_endpoint'
PG_PORT='your_postgres_port'
PG_DATABASE='your_database_name'
PG_SCHEMA='your_schema_name'
PG_USERNAME='your_postgres_username'
PG_PASSWORD='your_postgres_password'

# media metadata API credentials
OMDB_BASE_URL='https://www.omdbapi.com/'
OMDB_API_KEY='your_omdb_api_key'

# RSS feed URLs
# Generate movie RSS feed from: https://yts.torrentbay.st/rss-guide
MOVIE_RSS_URL='your_movie_rss_url'
# Generate TV show RSS feed from: https://showrss.info/
TV_SHOW_RSS_URL='your_tv_show_rss_url'

# directory paths
DOWNLOAD_DIR='path_to_download_directory'
MOVIE_DIR='path_to_movie_library'
TV_SHOW_DIR='path_to_tv_show_library'

# time in seconds to wait until media items are removed from transmission after completion
CLEANUP_DELAY=<integer number of seconds>
```

additional configuration files:
- `config/filter-parameters.json`: configure media filtering parameters
- `config/string-special-conditions.yaml`: define special string handling conditions for edge case filenames

## usage

execute the pipelines using the command line interface:

### for movies:
```
python main.py movie_pipeline
```

### for TV shows:
```
python main.py tv_show_pipeline
```

### for TV seasons:
```
python main.py tv_season_pipeline
```

### debug mode
add the `--debug` flag to enable verbose logging:
```
python main.py movie_pipeline --debug
```

## core modules

the application follows a sequential pipeline process:

1. `01_rss_ingest.py`: ingests torrent files from RSS feeds
2. `02_collect.py`: collects media information
3. `03_parse.py`: parses media details
4. `04_metadata_collection.py`: retrieves and stores metadata for the media
5. `05_filter.py`: filters media based on configured parameters
6. `06_initiate.py`: initiates the download process
7. `07_download_check.py`: monitors download progress
8. `08_transfer.py`: transfers completed downloads to the media library
9. `09_cleanup.py`: performs cleanup operations

## utility modules

various utility modules in the `utils` directory provide supporting functions:
- `error_handling.py`: contains scripts to run to perform ad-hoc pipeline maintenance
- `local_file_operations.py`: handles local file operations
- `parse_element.py`: parses XML/RSS elements
- `rpcf.py`: remote procedure calls
- `sql.py`: database operations
 

## license

this project is licensed under the MIT License