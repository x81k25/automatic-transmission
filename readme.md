Sure, here is an updated version of your `README.md` file:

```markdown
# Automatic Transmission

## Overview
Automatic Transmission is a Python-based project designed to manage and automate the downloading, organizing, and processing of TV shows and movies. The project leverages various libraries and tools to interact with remote servers, manage media files, and control the Transmission service.

## Features
- **SSH Command Execution**: Execute commands on remote servers using SSH.
- **Transmission Service Management**: Start, stop, and restart the Transmission service.
- **Media File Management**: Move and organize TV shows and movies into designated directories.
- **Directory Management**: Create directories if they do not exist.
- **Media Dump Management**: Print and purge the contents of the media dump directory.

## Requirements
The project requires the following Python packages:

- `feedparser`
- `python-dotenv`
- `pandas`
- `paramiko`
- `transmission-rpc`
- `tmdbv3api`

Ensure these packages are listed in your `requirements.txt` file.

## Installation
1. Clone the repository:
    ```shell
    git clone https://github.com/yourusername/automatic-transmission.git
    ```
2. Navigate to the project directory:
    ```shell
    cd automatic-transmission
    ```
3. Create a virtual environment:
    ```shell
    python -m venv venv
    ```
4. Activate the virtual environment:
    - On Windows:
        ```shell
        venv\Scripts\activate
        ```
    - On macOS/Linux:
        ```shell
        source venv/bin/activate
        ```
5. Install the required packages:
    ```shell
    pip install -r requirements.txt
    ```

## Usage
### Running the Main Script
To execute the `tv_show_pipeline()` function from the Windows shell, use the following command:
```shell
python C:/Users/jpeck/py/automatic-transmission/main.py tv_show_pipeline
```

### Managing Transmission Service
- **Get Transmission Service Status**:
    ```python
    get_transmission_service_status()
    ```
- **Start Transmission Service**:
    ```python
    trans_start()
    ```
- **Stop Transmission Service**:
    ```python
    trans_stop()
    ```
- **Restart Transmission Service**:
    ```python
    trans_restart()
    ```

### Moving Media Files
- **Move Movie**:
    ```python
    move_movie(file_name, DOWNLOAD_DIR, movie_dir)
    ```
- **Move TV Show**:
    ```python
    move_tv_show(download_dir, tv_show_dir, file_name, tv_show_name, release_year, season)
    ```

### Managing Media Dump
- **Print Media Dump Contents**:
    ```python
    print_dump_contents()
    ```
- **Purge Media Dump**:
    ```python
    purge_media_dump()
    ```

## Configuration
Ensure you have a `.env` file in the project root with the necessary environment variables for SSH connection and other configurations.

## License
This project is licensed under the MIT License. See the `LICENSE` file for more details.
```

Make sure to replace `yourusername` with your actual GitHub username in the repository URL.