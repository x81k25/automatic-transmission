# standard library imports
import logging
from pathlib import Path
import shutil
from typing import Union

# ------------------------------------------------------------------------------
# supporting functions
# ------------------------------------------------------------------------------

def set_permissions_and_ownership(
    path: Union[Path, str],
    user: str = "x81-media",
    group: str = "x81-group",
    mode: int = 0o775
) -> None:
    """
    Recursively sets permissions and ownership for a file or directory.
    
    Args:
        path (Union[Path, str]): Path to the file or directory
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
# functions that will be called directly by functions in the core dir
# ------------------------------------------------------------------------------

def move_movie_local(
    download_dir: Union[str, Path],
    movie_dir: Union[str, Path],
    dir_or_file_name: str
) -> None:
    """
    Copies a movie file or directory from download directory to movie directory.
    Maintains permissions (775) and ownership (x81-media:x81-group).
    
    Args:
        download_dir: Source directory where the file is downloaded
        movie_dir: Destination directory for movies
        dir_or_file_name: Name of the file or directory to copy
        
    Raises:
        ValueError: If paths are invalid or source doesn't exist
        PermissionError: If unable to set required permissions/ownership
        OSError: For file operation failures
    """
    try:
        # Convert to Path objects and validate
        paths = validate_paths(download_dir, movie_dir)
        source_dir, dest_dir = paths
        
        # Construct full paths
        source_path = source_dir / dir_or_file_name
        dest_path = dest_dir / dir_or_file_name
        
        # Verify source exists
        if not source_path.exists():
            raise ValueError(f"Source does not exist: {source_path}")
            
        logging.debug(f"Starting copy operation from {source_path} to {dest_path}")
        
        try:
            # Remove destination if it exists
            if dest_path.exists():
                if dest_path.is_file():
                    dest_path.unlink()
                else:
                    shutil.rmtree(dest_path)
                logging.debug(f"Removed existing destination: {dest_path}")
            
            # Copy based on type
            if source_path.is_file():
                logging.debug(f"Copying file: {source_path}")
                shutil.copy2(source_path, dest_path)
            else:
                logging.debug(f"Copying directory: {source_path}")
                shutil.copytree(source_path, dest_path)
                
            # Set permissions and ownership
            set_permissions_and_ownership(dest_path)
            
            logging.debug(f"Successfully copied {source_path} to {dest_path}")
            
        except (shutil.Error, OSError) as e:
            error_msg = f"Failed to copy {source_path} to {dest_path}: {str(e)}"
            logging.error(error_msg)
            
            # Clean up partial copy if it exists
            if dest_path.exists():
                try:
                    if dest_path.is_file():
                        dest_path.unlink()
                    else:
                        shutil.rmtree(dest_path)
                    logging.debug(f"Cleaned up partial copy at {dest_path}")
                except OSError as cleanup_error:
                    logging.error(f"Failed to clean up partial copy at {dest_path}: {str(cleanup_error)}")
                    
            raise OSError(error_msg)
            
    except Exception as e:
        # Log any other unexpected errors
        error_msg = f"Unexpected error in move_movie: {str(e)}"
        logging.error(error_msg)
        raise


def move_tv_show_local(
    download_dir: Union[str, Path],
    tv_show_dir: Union[str, Path],
    dir_or_file_name: str,
    tv_show_name: str,
    release_year: int,
    season: int
) -> None:
    """
    Copies a TV show file or directory to the appropriate season folder.
    Maintains permissions (775) and ownership (x81-media:x81-group).
    Creates season directories if they don't exist.
    
    Args:
        download_dir: Directory where the file is downloaded
        tv_show_dir: Base directory for TV shows
        dir_or_file_name: Name of the file or directory to copy
        tv_show_name: Name of the TV show
        release_year: Year the TV show was released
        season: Season number
        
    Raises:
        ValueError: If paths are invalid or source doesn't exist
        PermissionError: If unable to set required permissions/ownership
        OSError: For file operation failures
    """
    try:
        # Convert to Path objects and validate base directories
        paths = validate_paths(download_dir, tv_show_dir)
        source_dir, base_tv_dir = paths
        
        # Construct source path
        source_path = source_dir / dir_or_file_name
        
        # Verify source exists
        if not source_path.exists():
            raise ValueError(f"Source does not exist: {source_path}")
            
        # Generate destination path components
        show_name_formatted = tv_show_name.replace(' ', '-').lower()
        show_year_formatted = str(int(release_year))
        season_formatted = f"s{str(int(season)).zfill(2)}"
        
        # Construct full destination path
        show_dir = base_tv_dir / f"{show_name_formatted}-{show_year_formatted}"
        season_dir = show_dir / season_formatted
        dest_path = season_dir / dir_or_file_name
        
        logging.debug(
            f"Preparing to copy {source_path} to {dest_path}\n"
            f"Show: {show_name_formatted}\n"
            f"Year: {show_year_formatted}\n"
            f"Season: {season_formatted}"
        )
        
        try:
            # Create show and season directories if they don't exist
            season_dir.mkdir(parents=True, exist_ok=True)
            
            # Set permissions for newly created directories
            if show_dir.exists():
                set_permissions_and_ownership(show_dir)
            if season_dir.exists():
                set_permissions_and_ownership(season_dir)
            
            # Remove destination if it exists
            if dest_path.exists():
                if dest_path.is_file():
                    dest_path.unlink()
                else:
                    shutil.rmtree(dest_path)
                logging.debug(f"Removed existing destination: {dest_path}")
            
            # Copy based on type
            if source_path.is_file():
                logging.debug(f"Copying file: {source_path}")
                shutil.copy2(source_path, dest_path)
            else:
                logging.debug(f"Copying directory: {source_path}")
                shutil.copytree(source_path, dest_path)
                
            # Set permissions and ownership for the copied content
            set_permissions_and_ownership(dest_path)
            
            logging.debug(f"Successfully copied {source_path} to {dest_path}")
            
        except (shutil.Error, OSError) as e:
            error_msg = f"Failed to copy {source_path} to {dest_path}: {str(e)}"
            logging.error(error_msg)
            
            # Clean up partial copy if it exists
            if dest_path.exists():
                try:
                    if dest_path.is_file():
                        dest_path.unlink()
                    else:
                        shutil.rmtree(dest_path)
                    logging.debug(f"Cleaned up partial copy at {dest_path}")
                except OSError as cleanup_error:
                    logging.error(f"Failed to clean up partial copy at {dest_path}: {str(cleanup_error)}")
                    
            raise OSError(error_msg)
            
    except Exception as e:
        # Log any other unexpected errors
        error_msg = f"Unexpected error in move_tv_show: {str(e)}"
        logging.error(error_msg)
        raise


def move_tv_season_local(
    download_dir: Union[str, Path],
    tv_show_dir: Union[str, Path],
    dir_name: str,
    tv_show_name: str,
    release_year: int,
    season: int
) -> None:
    """
    Copies the contents of a TV season directory to the appropriate season folder.
    Maintains permissions (775) and ownership (x81-media:x81-group).
    Creates season directories if they don't exist.
    
    Args:
        download_dir: Directory where the files are downloaded
        tv_show_dir: Base directory for TV shows
        dir_name: Name of the directory containing season contents
        tv_show_name: Name of the TV show
        release_year: Year the TV show was released
        season: Season number
        
    Raises:
        ValueError: If paths are invalid or source doesn't exist
        PermissionError: If unable to set required permissions/ownership
        OSError: For file operation failures
    """
    try:
        # Convert to Path objects and validate base directories
        paths = validate_paths(download_dir, tv_show_dir)
        source_dir, base_tv_dir = paths
        
        # Construct source path
        source_path = source_dir / dir_name
        
        # Verify source exists and is a directory
        if not source_path.exists():
            raise ValueError(f"Source does not exist: {source_path}")
        if not source_path.is_dir():
            raise ValueError(f"Source must be a directory: {source_path}")
            
        # Generate destination path components
        show_name_formatted = tv_show_name.replace(' ', '-').lower()
        show_year_formatted = str(int(release_year))
        season_formatted = f"s{str(int(season)).zfill(2)}"
        
        # Construct full destination path
        show_dir = base_tv_dir / f"{show_name_formatted}-{show_year_formatted}"
        season_dir = show_dir / season_formatted
        
        logging.debug(
            f"Preparing to copy contents from {source_path} to {season_dir}\n"
            f"Show: {show_name_formatted}\n"
            f"Year: {show_year_formatted}\n"
            f"Season: {season_formatted}"
        )
        
        try:
            # Create show and season directories if they don't exist
            season_dir.mkdir(parents=True, exist_ok=True)
            
            # Set permissions for newly created directories
            if show_dir.exists():
                set_permissions_and_ownership(show_dir)
            if season_dir.exists():
                set_permissions_and_ownership(season_dir)
            
            # Copy contents of source directory
            for item in source_path.iterdir():
                dest_path = season_dir / item.name
                
                # Remove destination if it exists
                if dest_path.exists():
                    if dest_path.is_file():
                        dest_path.unlink()
                    else:
                        shutil.rmtree(dest_path)
                    logging.debug(f"Removed existing destination: {dest_path}")
                
                # Copy based on type
                if item.is_file():
                    logging.debug(f"Copying file: {item}")
                    shutil.copy2(item, dest_path)
                else:
                    logging.debug(f"Copying directory: {item}")
                    shutil.copytree(item, dest_path)
                    
                # Set permissions and ownership for each copied item
                set_permissions_and_ownership(dest_path)
                
            logging.debug(f"Successfully copied contents from {source_path} to {season_dir}")
            
        except (shutil.Error, OSError) as e:
            error_msg = f"Failed to copy contents from {source_path} to {season_dir}: {str(e)}"
            logging.error(error_msg)
            
            # Clean up partial copies
            for item in season_dir.iterdir():
                try:
                    if item.is_file():
                        item.unlink()
                    else:
                        shutil.rmtree(item)
                    logging.debug(f"Cleaned up partial copy at {item}")
                except OSError as cleanup_error:
                    logging.error(f"Failed to clean up partial copy at {item}: {str(cleanup_error)}")
                    
            raise OSError(error_msg)
            
    except Exception as e:
        # Log any other unexpected errors
        error_msg = f"Unexpected error in move_tv_season: {str(e)}"
        logging.error(error_msg)
        raise

# ------------------------------------------------------------------------------
# end of sshf.py
# ------------------------------------------------------------------------------