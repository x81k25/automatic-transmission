# standard library imports
import logging
import os
from pathlib import Path, PurePosixPath
import shutil
from typing import Union

# third party library imports
from dotenv import load_dotenv
import re

# ------------------------------------------------------------------------------
# supporting functions
# ------------------------------------------------------------------------------

def set_permissions_and_ownership(
    path: Union[PurePosixPath, str],
    mode: int = 0o775
) -> None:
    """
    Recursively sets permissions and ownership for a file or directory.
    AT_UID and AT_GID are read from environment variables.

    :param path: Path to the file or directory
    :param mode: Permission mode in octal (default: 775)
    :return: None
    :raises ValueError: If path doesn't exist or AT_UID/AT_GID env vars are invalid
    :raises PermissionError: If unable to set permissions or ownership
    :raises OSError: For other operating system related errors
    """
    # collect UID:GID from .env
    load_dotenv()
    try:
        uid = int(os.getenv("AT_UID", "1005"))
        gid = int(os.getenv("AT_GID", "1001"))
    except ValueError as e:
        raise ValueError(f"Invalid AT_UID/AT_GID in environment variables: {e}")

    # Convert to Path object if string
    path = Path(path)
    
    # Validate path exists
    if not path.exists():
        raise ValueError(f"Path does not exist: {path}")
    
    try:
        # Set ownership and permissions for the path itself
        shutil.chown(path, user=uid, group=gid)
        path.chmod(mode)
        
        logging.debug(f"Set permissions and ownership for: {path}")
        
        # If it's a directory, recursively process contents
        if path.is_dir():
            for item in path.rglob('*'):
                try:
                    shutil.chown(item, user=uid, group=gid)
                    item.chmod(mode)
                    logging.debug(f"Set permissions and ownership for: {item}")
                except (PermissionError, OSError) as e:
                    error_msg = f"Failed to set permissions/ownership for {item}: {str(e)}"
                    logging.error(error_msg)
                    raise PermissionError(error_msg)
                    
    except (PermissionError, OSError) as e:
        error_msg = f"Failed to set initial permissions/ownership for {path}: {str(e)}"
        logging.error(error_msg)
        raise PermissionError(error_msg)


# ------------------------------------------------------------------------------
# functions to generate proper file paths
# ------------------------------------------------------------------------------

# movies
def generate_movie_target_path(
    movie_title: str,
    release_year: int,
    resolution: str = None,
    video_codec: str = None
) -> str:
    """
    generates the file_name that will the media item will be written as
        when moved to the media library

    :param movie_title: movie title
    :param release_year: year movie war released
    :param resolution: resolution of media file
    :param video_codec: video_codec, if extracted
    :return: Path object of target dir
    :raises ValueError: If required parameters are invalid
    """
    # Validate required parameters
    if not movie_title or not isinstance(movie_title, str):
        raise ValueError("movie_title must be a non-empty string")

    if not release_year or not isinstance(release_year, int):
        raise ValueError("release_year must be a valid integer")

    try:
        # Clean and format movie title
        cleaned_title = movie_title.lower()
        # replace any non-alphanumeric characters (except spaces) with spaces
        cleaned_title = re.sub(r'[^\w\s]', ' ', cleaned_title)
        # replace multiple whitespace with single space
        cleaned_title = re.sub(r'\s+', ' ', cleaned_title)
        # replace spaces with hyphens
        cleaned_title = cleaned_title.replace(' ', '-')
        # replace multiple consecutive hyphens with single hyphen
        cleaned_title = re.sub(r'-+', '-', cleaned_title)
        # remove leading and trailing hyphens
        cleaned_title = cleaned_title.strip('-')

        if not cleaned_title:
            raise ValueError("movie_title resulted in empty string after cleaning")

        # Format year
        year_str = str(release_year).strip()

        # Build target path (resolution is optional)
        if resolution and isinstance(resolution, str) and resolution.strip():
            resolution_str = resolution.lower().strip()
            movie_target_path = f"{cleaned_title}-{year_str}-{resolution_str}"
        else:
            movie_target_path = f"{cleaned_title}-{year_str}"

        # Handle video codec if provided
        if video_codec is not None and isinstance(video_codec, str):
            try:
                # Clean the video codec
                codec_clean = video_codec.strip().strip('"')

                if codec_clean:  # Only process if not empty after stripping
                    # Convert H.265 and H 265 variations to H265
                    codec_clean = re.sub(r'H[\.\s]?265', 'h265', codec_clean, flags=re.IGNORECASE)
                    # Keep x265 as is (case insensitive, but maintain lowercase)
                    codec_clean = re.sub(r'^x265$', 'x265', codec_clean, flags=re.IGNORECASE)

                    # Only add codec if it matches expected values
                    if codec_clean.lower() in ("x265", "h265"):
                        movie_target_path = f"{movie_target_path}-{codec_clean}"

            except Exception as e:
                # Log the codec processing error but continue without it
                logging.warning(f"Error processing video_codec '{video_codec}': {e}")

        return movie_target_path

    except Exception as e:
        raise ValueError(f"Failed to generate movie target path: {e}")


# seasons
def generate_tv_season_parent_path(
    root_dir: str,
    tv_show_name: str,
    release_year: int,
) -> PurePosixPath:
    """
    generates the name of the parent directory for a tv_season

    :param root_dir: root dir for all tv content
    :param tv_show_name: tv show name string, if tv_season
    :param release_year: release year of tv_season
    :return: PurePosixPath object of parent dir
    :raises ValueError: If required parameters are invalid
    """
    # Validate required parameters
    if not root_dir or not isinstance(root_dir, str):
        raise ValueError("root_dir must be a non-empty string")

    if not tv_show_name or not isinstance(tv_show_name, str):
        raise ValueError("tv_show_name must be a non-empty string")

    if not release_year or not isinstance(release_year, int):
        raise ValueError("release_year must be a valid integer")

    try:
        # Apply title cleaning logic to show name
        show_name_clean = tv_show_name.lower()
        show_name_clean = re.sub(r'[^\w\s]', ' ', show_name_clean)
        show_name_clean = re.sub(r'\s+', ' ', show_name_clean)
        show_name_clean = show_name_clean.replace(' ', '-')
        show_name_clean = re.sub(r'-+', '-', show_name_clean)
        show_name_clean = show_name_clean.strip('-')

        if not show_name_clean:
            raise ValueError("tv_show_name resulted in empty string after cleaning")

        # Create show folder with year
        show_folder = f"{show_name_clean}-{release_year}"

        # Use PurePosixPath operations to safely join
        return PurePosixPath(root_dir) / show_folder

    except Exception as e:
        raise ValueError(f"Failed to generate TV season parent path: {e}")


def generate_tv_season_target_path(
    season: int
) -> str:
    """
    generates the name of the target directory for a tv_season

    :param season: season of tv_season
    :return: string representation of target dir
    :raises ValueError: If season parameter is invalid
    """
    # Validate required parameters
    if season is None or not isinstance(season, int):
        raise ValueError("season must be a valid integer")

    if season < 0:
        raise ValueError("season cannot be negative")

    if season > 9999:
        raise ValueError("season must be 9999 or less")

    try:
        # Create season folder - determine padding based on season number
        if season < 100:
            season_path = f"s{season:02d}"  # s01, s02, etc.
        elif season < 1000:
            season_path = f"s{season:03d}"  # s001, s002, etc.
        else:
            season_path = f"s{season:04d}"  # s0001, s0002, etc.

        return season_path

    except Exception as e:
        raise ValueError(f"Failed to generate TV season target path: {e}")


# tv shows
def generate_tv_show_parent_path(
    root_dir: str,
    tv_show_name: str,
    release_year: int,
    season: int,
) -> PurePosixPath:
    """
    generates the name of the parent directory for a tv_show or tv_season

    :param root_dir: root dir for all tv content
    :param tv_show_name: tv show name string, if tv_show or tv_season
    :param release_year: release year of tv_show or tv_season
    :param season: season of tv_show or tv_season
    :return: PurePosixPath object of parent dir
    :raises ValueError: If required parameters are invalid
    """
    # Validate required parameters
    if not root_dir or not isinstance(root_dir, str):
        raise ValueError("root_dir must be a non-empty string")

    if not tv_show_name or not isinstance(tv_show_name, str):
        raise ValueError("tv_show_name must be a non-empty string")

    if not release_year or not isinstance(release_year, int):
        raise ValueError("release_year must be a valid integer")

    if season is None or not isinstance(season, int):
        raise ValueError("season must be a valid integer")

    if season < 0:
        raise ValueError("season cannot be negative")

    if season > 9999:
        raise ValueError("season must be 9999 or less")

    try:
        # Apply title cleaning logic to show name
        show_name_clean = tv_show_name.lower()
        show_name_clean = re.sub(r'[^\w\s]', ' ', show_name_clean)
        show_name_clean = re.sub(r'\s+', ' ', show_name_clean)
        show_name_clean = show_name_clean.replace(' ', '-')
        show_name_clean = re.sub(r'-+', '-', show_name_clean)
        show_name_clean = show_name_clean.strip('-')

        if not show_name_clean:
            raise ValueError("tv_show_name resulted in empty string after cleaning")

        # Create show folder with year
        show_folder = f"{show_name_clean}-{release_year}"

        # Create season folder - determine padding based on season number
        if season < 100:
            season_folder = f"s{season:02d}"  # s01, s02, etc.
        elif season < 1000:
            season_folder = f"s{season:03d}"  # s001, s002, etc.
        else:
            season_folder = f"s{season:04d}"  # s0001, s0002, etc.

        # Use PurePosixPath operations to safely join
        return PurePosixPath(root_dir) / show_folder / season_folder

    except Exception as e:
        raise ValueError(f"Failed to generate TV show parent path: {e}")


def generate_tv_show_target_path(
   season: int,
   episode: int,
) -> str:
   """
   generates the name of the target directory for a tv_show

   :param season: season of tv_show
   :param episode: episode number of tv_show
   :return: str representation of target dir
   :raises ValueError: If parameters are invalid
   """
   # Validate required parameters
   if season is None or not isinstance(season, int):
       raise ValueError("season must be a valid integer")

   if episode is None or not isinstance(episode, int):
       raise ValueError("episode must be a valid integer")

   if season < 0:
       raise ValueError("season cannot be negative")

   if episode < 1:
       raise ValueError("episode must be greater than 0")

   if season > 9999:
       raise ValueError("season must be 9999 or less")

   if episode > 9999:
       raise ValueError("episode must be 9999 or less")

   try:
       # Determine padding for season number
       if season < 100:
           season_str = f"{season:02d}"
       elif season < 1000:
           season_str = f"{season:03d}"
       else:
           season_str = f"{season:04d}"

       # Determine padding for episode number
       if episode < 100:
           episode_str = f"{episode:02d}"
       elif episode < 1000:
           episode_str = f"{episode:03d}"
       else:
           episode_str = f"{episode:04d}"

       return f"s{season_str}e{episode_str}"

   except Exception as e:
       raise ValueError(f"Failed to generate TV show target path: {e}")


# ------------------------------------------------------------------------------
# functions to transfer files
# ------------------------------------------------------------------------------

def move_dir_or_file(
    full_original_path: PurePosixPath,
    full_target_path: PurePosixPath,
    merge: bool = False
) -> None:
    """
    Copies a file or directory from original path to target path.
    Maintains permissions (775) and ownership (x81-media:x81-group).

    If original path is a file: creates target directory and copies file into it
    If original path is a directory: copies entire directory to target path

    Args:
        full_original_path: Path to the source file or directory
        full_target_path: Path to the target directory (always treated as directory)
        merge: If True, merge source contents into existing target directory
               instead of overwriting. Individual files are still overwritten.

    Raises:
        ValueError: If paths are invalid or source doesn't exist
        PermissionError: If unable to set required permissions/ownership
        OSError: For file operation failures
    """
    try:
        # Convert to Path objects
        source_path = Path(full_original_path)
        target_path = Path(full_target_path)

        # Verify source exists
        if not source_path.exists():
            raise ValueError(f"Source does not exist: {source_path}")

        # Ensure parent directories of target path exist (create them if needed)
        # Track which directories we actually create so we only set permissions on those
        dirs_to_create = []
        current_check = target_path.parent

        # Find which directories need to be created
        while not current_check.exists() and current_check != current_check.parent:
            dirs_to_create.append(current_check)
            current_check = current_check.parent

        # Create the directories
        target_path.parent.mkdir(parents=True, exist_ok=True)

        # Set permissions only on directories we actually created
        for created_dir in reversed(dirs_to_create):  # Set permissions from top down
            if created_dir.exists():
                try:
                    set_permissions_and_ownership(created_dir)
                    logging.debug(f"Set permissions on created directory: {created_dir}")
                except (PermissionError, OSError) as e:
                    logging.warning(f"Could not set permissions on created directory {created_dir}: {str(e)}")
                    # Stop trying to set permissions on parent directories if we hit permission issues
                    break

        logging.debug(f"Starting copy operation from {source_path} to {target_path}")

        try:
            # Determine if source is file or directory
            if source_path.is_file():
                logging.debug(f"Source is a file: {source_path}")

                # Create target directory
                target_path.mkdir(parents=True, exist_ok=True)
                set_permissions_and_ownership(target_path)

                # Construct destination file path (keep original filename)
                dest_file_path = target_path / source_path.name

                # Remove destination file if it exists
                if dest_file_path.exists():
                    dest_file_path.unlink()
                    logging.debug(f"Removed existing destination file: {dest_file_path}")

                # Copy file
                logging.debug(f"Copying file to: {dest_file_path}")
                shutil.copy2(source_path, dest_file_path)

                # Set permissions on copied file
                set_permissions_and_ownership(dest_file_path)

                logging.debug(f"Successfully copied file {source_path} to {dest_file_path}")

            elif source_path.is_dir():
                logging.debug(f"Source is a directory: {source_path}")

                if merge and target_path.exists():
                    # Merge mode: copy contents into existing directory
                    logging.debug(f"Merging directory contents into: {target_path}")
                    for item in source_path.iterdir():
                        dest_item = target_path / item.name
                        if item.is_file():
                            # Overwrite individual files
                            if dest_item.exists():
                                dest_item.unlink()
                            shutil.copy2(item, dest_item)
                            set_permissions_and_ownership(dest_item)
                        elif item.is_dir():
                            # Recursively merge subdirectories
                            if dest_item.exists():
                                shutil.rmtree(dest_item)
                            shutil.copytree(item, dest_item)
                            set_permissions_and_ownership(dest_item)
                    logging.debug(f"Successfully merged {source_path} into {target_path}")
                else:
                    # Overwrite mode: remove destination if it exists
                    if target_path.exists():
                        shutil.rmtree(target_path)
                        logging.debug(f"Removed existing destination directory: {target_path}")

                    # Copy entire directory
                    logging.debug(f"Copying directory to: {target_path}")
                    shutil.copytree(source_path, target_path)

                    # Set permissions on copied directory and all contents
                    set_permissions_and_ownership(target_path)

                    logging.debug(f"Successfully copied directory {source_path} to {target_path}")

            else:
                raise ValueError(f"Source path is neither file nor directory: {source_path}")

        except (shutil.Error, OSError) as e:
            error_msg = f"Failed to copy {source_path} to {target_path}: {str(e)}"
            logging.error(error_msg)

            # Clean up partial copy if it exists
            if target_path.exists():
                try:
                    if target_path.is_file():
                        target_path.unlink()
                    else:
                        shutil.rmtree(target_path)
                    logging.debug(f"Cleaned up partial copy at {target_path}")
                except OSError as cleanup_error:
                    logging.error(f"Failed to clean up partial copy at {target_path}: {str(cleanup_error)}")

            raise OSError(error_msg)

    except Exception as e:
        # Log any other unexpected errors
        error_msg = f"Unexpected error in move_dir_or_file: {str(e)}"
        logging.error(error_msg)
        raise


# ------------------------------------------------------------------------------
# end of local_file_operations.py
# ------------------------------------------------------------------------------