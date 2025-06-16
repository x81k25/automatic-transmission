# Containerization

Docker containerization for the automatic-transmission pipeline. All containers run as UID:1005 GID:1001.

## Prerequisites

- Docker and Docker Compose installed
- `.env` file in project root with all required environment variables

## CI/CD with GitHub Actions

This project uses GitHub Actions to automatically build and push Docker images to GitHub Container Registry (ghcr.io).

### Automated Builds

- **Trigger**: Pushes to `dev-agent`, `dev`, `stg`, `main` branches or PRs targeting `dev`, `stg`, `main`
- **Registry**: Images are pushed to `ghcr.io/x81k25/automatic-transmission/`
- **Tagging Strategy**:
  - Branch builds: `ghcr.io/x81k25/automatic-transmission/[image-name]:[branch-name]`
  - PR builds: `ghcr.io/x81k25/automatic-transmission/[image-name]:pr-[number]`
  - Main branch: Also tagged as `latest`
  - All builds: Also tagged with commit SHA

### Using CI/CD Images

To use images from the CI/CD pipeline instead of building locally:

```bash
# Set the BASE_IMAGE environment variable before running docker-compose
export BASE_IMAGE=ghcr.io/x81k25/automatic-transmission/at-base:dev-agent

# Then run services as normal
docker-compose -f containerization/docker-compose.yml run --rm rss-ingest
```

## File Structure

```
containerization/
├── dockerfile.00_base           # Base image with Python 3.12 and dependencies
├── dockerfile.01_rss_ingest     # RSS feed ingestion module
├── dockerfile.02_collect        # Torrent collection module
├── dockerfile.03_parse          # Media parsing module
├── dockerfile.04_file_filtration    # File filtering module
├── dockerfile.05_metadata_collection # Metadata enrichment module
├── dockerfile.06_media_filtration   # Media filtering module
├── dockerfile.07_initiation     # Download initiation module
├── dockerfile.08_download_check # Download status checking module
├── dockerfile.09_transfer       # File transfer module
├── dockerfile.10_cleanup        # Cleanup module
├── docker-compose.yml           # Docker Compose configuration
└── readme.md                    # This file
```

## Build Commands

### Build All Images
```bash
# Build all images at once
docker-compose -f containerization/docker-compose.yml --profile build-only build
```

### Build Individual Images
```bash
# Build base image (required for all other images)
docker build -f containerization/dockerfile.00_base -t at-base:latest .

# Build specific module image (example: RSS ingest)
docker build -f containerization/dockerfile.01_rss_ingest -t at-rss-ingest:latest .
```

## Run Commands

### Run Complete Pipeline
```bash
# Run all pipeline stages sequentially
docker-compose -f containerization/docker-compose.yml --profile pipeline up

# Run with build (rebuilds images if needed)
docker-compose -f containerization/docker-compose.yml --profile pipeline up --build
```

### Run Individual Modules
```bash
# Run specific module (example: RSS ingest)
docker-compose -f containerization/docker-compose.yml run --rm rss-ingest

# Run with custom environment variables
docker-compose -f containerization/docker-compose.yml run --rm -e DEBUG_MODE=true rss-ingest

# Run multiple specific modules
docker-compose -f containerization/docker-compose.yml --profile rss-ingest --profile collect up
```

### Available Profiles
- `build-only`: Build base image only
- `pipeline`: Run complete pipeline
- `rss-ingest`: Run RSS ingestion module
- `collect`: Run collection module
- `parse`: Run parsing module
- `file-filtration`: Run file filtration module
- `metadata-collection`: Run metadata collection module
- `media-filtration`: Run media filtration module
- `initiation`: Run download initiation module
- `download-check`: Run download check module
- `transfer`: Run file transfer module
- `cleanup`: Run cleanup module

## Debugging

### View Container Logs
```bash
# View logs for specific service
docker-compose -f containerization/docker-compose.yml logs -f rss-ingest

# View logs for all services
docker-compose -f containerization/docker-compose.yml logs -f
```

### Shell Access
```bash
# Get shell access to a container
docker run --rm -it --env-file .env --entrypoint /bin/bash at-rss-ingest:latest

# Or using docker-compose
docker-compose -f containerization/docker-compose.yml run --rm --entrypoint /bin/bash rss-ingest
```

### Debug Mode
```bash
# Run with debug logging
docker-compose -f containerization/docker-compose.yml run --rm -e LOG_LEVEL=DEBUG rss-ingest

# Run with Python debugger
docker-compose -f containerization/docker-compose.yml run --rm -e PYTHONBREAKPOINT=ipdb.set_trace rss-ingest
```

## Environment Variables

All containers require a `.env` file in the project root. Key variables include:

- `BATCH_SIZE`: Number of items to process per batch
- `DEBUG_MODE`: Enable debug logging (true/false)
- `DATABASE_URL`: PostgreSQL connection string
- `TRANSMISSION_HOST`: Transmission RPC host
- `TRANSMISSION_PORT`: Transmission RPC port
- `TRANSMISSION_USER`: Transmission RPC username
- `TRANSMISSION_PASS`: Transmission RPC password
- `TMDB_API_KEY`: TMDB API key for metadata
- `OMDB_API_KEY`: OMDb API key for metadata
- `REEL_API_KEY`: reel-driver API key
- Various directory paths for media organization

## Cleanup

```bash
# Stop all containers
docker-compose -f containerization/docker-compose.yml down

# Remove all project images
docker rmi $(docker images -q 'at-*')

# Clean up volumes and networks
docker-compose -f containerization/docker-compose.yml down -v
```

## Troubleshooting

### Permission Issues
All containers run as UID:1005 GID:1001. Ensure your host directories have appropriate permissions:
```bash
sudo chown -R 1005:1001 /path/to/media/directories
```

### Database Connection Issues
Ensure PostgreSQL is accessible from within containers:
- Check `DATABASE_URL` in `.env`
- Verify PostgreSQL is running and accepting connections
- Check firewall rules if database is on different host

### Build Failures
If builds fail:
1. Clear Docker cache: `docker system prune -a`
2. Check `requirements.txt` is present in project root
3. Ensure all source files are present in `src/` directory
4. Check Docker daemon logs for errors

### Module Failures
If a specific module fails:
1. Check logs: `docker-compose logs <service-name>`
2. Verify all required environment variables are set
3. Run in debug mode to see detailed errors
4. Check database connectivity and required tables exist

### CI/CD Issues

#### GitHub Actions Build Failures
- Check workflow logs in the Actions tab on GitHub
- Ensure repository has package write permissions enabled
- Verify branch protection rules aren't blocking the workflow

#### Registry Access Issues
- Images are public by default but can be made private
- To pull private images, authenticate with: `docker login ghcr.io -u USERNAME`
- Use a Personal Access Token (PAT) with `read:packages` scope as password

#### Image Cleanup
Old images are not automatically cleaned up. To manage registry storage:
- Delete old images manually via GitHub UI (Packages section)
- Use retention policies in repository settings
- Consider implementing automated cleanup in the workflow