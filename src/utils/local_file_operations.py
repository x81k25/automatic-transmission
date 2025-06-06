# standard library imports
import logging
from pathlib import Path, PurePosixPath
import shutil
from typing import Union

# third party library imports
import re

# ------------------------------------------------------------------------------
# supporting functions
# ------------------------------------------------------------------------------

def set_permissions_and_ownership(
    path: Union[PurePosixPath, str],
    user: str = "x81-media",
    group: str = "x81-group",
    mode: int = 0o775
) -> None:
    """
    Recursively sets permissions and ownership for a file or directory.
    
    Args:
        path (Union[PurePosixPath, str]): Path to the file or directory
        user (str): Username for ownership
        group (str): Group name for ownership
        mode (int): Permission mode in octal (default: 775)
        
    Raises:
        ValueError: If path doesn't exist
        PermissionError: If unable to set permissions or ownership
        OSError: For other operating system related errors
    """
    # Convert to Path object if string
    path = Path(path)
    
    # Validate path exists
    if not path.exists():
        raise ValueError(f"Path does not exist: {path}")
    
    try:
        # Set ownership and permissions for the path itself
        shutil.chown(path, user=user, group=group)
        path.chmod(mode)
        
        logging.debug(f"Set permissions and ownership for: {path}")
        
        # If it's a directory, recursively process contents
        if path.is_dir():
            for item in path.rglob('*'):
                try:
                    shutil.chown(item, user=user, group=group)
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


def validate_paths(*paths: Union[Path, str]) -> list[Path]:
    """
    Validates multiple paths, ensuring parent directories exist.
    Does not verify if the final path component exists.
    
    Args:
        *paths: Variable number of path arguments
        
    Returns:
        list[Path]: List of validated Path objects
        
    Raises:
        ValueError: If any parent directory doesn't exist
    """
    validated_paths = []
    
    for path in paths:
        # Convert to Path object if string
        path = Path(path)
        
        # Check if parent directory exists
        if not path.parent.exists():
            raise ValueError(f"Parent directory does not exist: {path.parent}")
            
        validated_paths.append(path)
        
    return validated_paths


# ------------------------------------------------------------------------------
# functions to generate proper file paths
# ------------------------------------------------------------------------------

# movies
def generate_movie_target_path(
    movie_title: str,
    release_year: int,
    resolution: str,
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
    """
    # convert to lowercase
    movie_title = movie_title.lower()
    # replace any non-alphanumeric characters (except spaces) with spaces
    movie_title = re.sub(r'[^\w\s]', ' ', movie_title)
    # replace multiple whitespace with single space
    movie_title = re.sub(r'\s+', ' ', movie_title)
    # replace spaces with hyphens
    movie_title = movie_title.replace(' ', '-')
    # replace multiple consecutive hyphens with single hyphen
    movie_title = re.sub(r'-+', '-', movie_title)
    # remove leading and trailing hyphens
    movie_title = movie_title.strip('-')

    release_year = str(release_year).strip()
    resolution = str(resolution).lower().strip()

    movie_target_path = (
        movie_title + "-" +
        release_year + "-" +
        resolution
    )

    video_codec = video_codec.strip('"')
    # Convert H.265 and H 265 variations to H265
    video_codec = re.sub(r'H[\.\s]?265', 'h265', video_codec, flags=re.IGNORECASE)
    # Keep x265 as is (case insensitive, but maintain lowercase)
    video_codec = re.sub(r'^x265$', 'x265', video_codec, flags=re.IGNORECASE)

    if video_codec in ("x265", "H265"):
        movie_target_path = movie_target_path + "-" + video_codec

    return movie_target_path

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
    """
    # Apply movie title logic to show name
    show_name_clean = tv_show_name.lower()
    show_name_clean = re.sub(r'[^\w\s]', ' ', show_name_clean)
    show_name_clean = re.sub(r'\s+', ' ', show_name_clean)
    show_name_clean = show_name_clean.replace(' ', '-')
    show_name_clean = re.sub(r'-+', '-', show_name_clean)
    show_name_clean = show_name_clean.strip('-')

    # Create show folder with year
    show_folder = f"{show_name_clean}-{release_year}"

    # Use PurePosixPath operations to safely join
    return PurePosixPath(root_dir) / show_folder


def generate_tv_season_target_path(
    season: int
) -> str:
    """
    generates the name of the target directory for a tv_season

    :param season: season of tv_season
    :return: PurePosixPath object of target dir
    """
    # Create season folder - determine padding based on season number
    if season < 100:
        season_path = f"s{season:02d}"  # s01, s02, etc.
    elif season < 1000:
        season_path = f"s{season:03d}"  # s001, s002, etc.
    else:
        season_path = f"s{season:04d}"  # s0001, s0002, etc.

    return season_path


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
    """
    # Apply movie title logic to show name
    show_name_clean = tv_show_name.lower()
    show_name_clean = re.sub(r'[^\w\s]', ' ', show_name_clean)
    show_name_clean = re.sub(r'\s+', ' ', show_name_clean)
    show_name_clean = show_name_clean.replace(' ', '-')
    show_name_clean = re.sub(r'-+', '-', show_name_clean)
    show_name_clean = show_name_clean.strip('-')

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


def generate_tv_show_target_path(
   season: int,
   episode: int,
) -> str:
   """
   generates the name of the target directory for a tv_show

   :param season: season of tv_show
   :param episode: episode number of tv_show
   :return: str representation of target dir
   """
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


# ------------------------------------------------------------------------------
# functions to transfer files
# ------------------------------------------------------------------------------

def move_dir_or_file(
    full_original_path: PurePosixPath,
    full_target_path: PurePosixPath
) -> None:
    """
    Copies a file or directory from original path to target path.
    Maintains permissions (775) and ownership (x81-media:x81-group).

    If original path is a file: creates target directory and copies file into it
    If original path is a directory: copies entire directory to target path

    Args:
        full_original_path: Path to the source file or directory
        full_target_path: Path to the target directory (always treated as directory)

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

                # Remove destination if it exists
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