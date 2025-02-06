# standard library imports
import logging
import os
import stat

# third-party imports
from dotenv import load_dotenv
import paramiko

# ------------------------------------------------------------------------------
# load environment variables and
# ------------------------------------------------------------------------------

# Load environment variables from .env file
load_dotenv()

# load environment variables for SSH connection details
ssh_hostname = os.getenv('SERVER_IP')
ssh_user = os.getenv('SSH_USER')
ssh_password = os.getenv('SSH_PASSWORD')
ssh_group = os.getenv('MEDIA_GROUP')
ssh_port = os.getenv('SSH_PORT')
download_dir = os.getenv('DOWNLOAD_DIR')

# ------------------------------------------------------------------------------
# low level functions to be used by other functions within this package
# ------------------------------------------------------------------------------

def get_client(
    hostname: str = ssh_hostname,
    port: int = ssh_port,
    username: str = ssh_user,
    password: str = ssh_password
):
    try:
        # instantiate client object
        client = paramiko.SSHClient()

        # Load system host keys (optional, can be disabled for local testing)
        client.load_system_host_keys()

        # Automatically add new host keys to known_hosts
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # try:
        # Connect to the server
        client.connect(
            hostname=hostname,
            port=port,
            username=username,
            password=password
        )

        return client
    except Exception as e:
        error_message = f"get_client error: {str(e)}"
        logging.error(error_message)
        raise Exception(error_message)


def ssh_command(command: str = 'uname -a'):
    """
    Execute a command on the server
    :param command: Command to execute
    :return:
    """
    # get ssh client
    client = get_client()

    # execute the command
    stdin, stdout, stderr = client.exec_command(command)

    # read either the output or the error message
    output = stdout.read().decode()
    error = stderr.read().decode()

    # if successful return the output
    # if error log and raise exception
    if output:
        client.close()
        return f"ssh command output: {output}"
    elif error:
        error_message = f"ssh_command error: \n" + \
            "command: " + command + "\n" + \
            "error: " + error
        logging.error(error_message)
        print(error_message)
        client.close()
        raise Exception(error_message)
    else:
        client.close()


def print_dump_contents():
    """
    print the contents of the media-dump directory
    :return: contents
    """
    command = f"ls -l {download_dir}"

    response = ssh_command(command)

    return response


def purge_media_dump():
    """
    remove all files from the media-dump directory
    :return:
    """
    command = 'rm -r /k/media/media-dump/*'

    ssh_command(command)


def get_transmission_service_status():
    """
    get the status of the transmission service
    :return:
    """
    return ssh_command('systemctl status transmission-daemon')

# ------------------------------------------------------------------------------
# file operation functions
# ------------------------------------------------------------------------------

def check_dir_or_file_exists(remote_path: str = None):
    """
    Check if a file exists on remote server using Paramiko.
    :param remote_path: remote_path to check for file existence
    :returns:
        tuple: (bool, str) - (exists, file_type)
        where file_type is one of: 'file', 'directory', 'link', or 'unknown'
    """
    ssh = None
    sftp = None

    try:
        # get ssh client
        ssh = get_client()

        # Open SFTP session
        sftp = ssh.open_sftp()

        try:
            # Get file attributes
            attr = sftp.stat(remote_path)

            # Determine file type
            if stat.S_ISREG(attr.st_mode):
                return True, 'file'
            elif stat.S_ISDIR(attr.st_mode):
                return True, 'directory'
            elif stat.S_ISLNK(attr.st_mode):
                return True, 'link'
            else:
                return True, 'unknown'
        except FileNotFoundError:
            return False, ''

    except Exception as e:
        error_message = f"check_file_exists error: \n" + \
            f"remote_path: {remote_path} \n" + \
            f"error: {str(e)}"
        logging.error(error_message)
        print(error_message)
        raise Exception(error_message)

    finally:
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()


def create_dir(remote_path: str) -> bool:
    """
        Creates a new directory on the remote server using Paramiko SFTP.
        Creates parent directories if they don't exist (like mkdir -p).
        :param remote_path: Full path of directory to create
        :return: bool - True if successful, False if failed
        """
    ssh = None
    sftp = None

    try:
        # Get SSH client
        ssh = get_client()

        # Open SFTP session
        sftp = ssh.open_sftp()

        # Split path into parts and reconstruct incrementally
        path_parts = remote_path.split('/')
        current_path = ''

        # Handle absolute paths starting with /
        if remote_path.startswith('/'):
            current_path = '/'

        # Create each directory in path if it doesn't exist
        for part in path_parts:
            if not part:  # Skip empty parts
                continue

            current_path = current_path + part + '/'
            try:
                sftp.stat(current_path)
            except FileNotFoundError:
                sftp.mkdir(current_path)

        return True

    except Exception as e:
        error_message = f"create_remote_directory error: \n" + \
            f"remote_path: {remote_path} \n" + \
            f"error: {str(e)}"
        logging.error(error_message)
        print(error_message)
        raise Exception(error_message)

    finally:
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()


def copy_dir(
    source_dir: str,
    destination_dir: str,
    dir_name: str
) -> bool:
    """
    Copies a directory and all its contents recursively using cp command.
    If the destination directory or any of its contents already exist, they will be overwritten.
    The original directory and contents remain unchanged. Sets 775 permissions and specified
    ownership for all copied files and directories.

    :param source_dir: Base dir where the source directory is located
    :param destination_dir: Base dir where the directory should be copied to
    :param dir_name: Name of the directory to copy
    :return: bool - True if successful, False if failed
    """
    try:
        # Construct full paths, ensuring no trailing slashes
        full_source = f"{source_dir.rstrip('/')}/{dir_name}"
        full_destination = f"{destination_dir.rstrip('/')}/{dir_name}"

        # Escape single quotes in paths for shell commands
        escaped_source = full_source.replace("'", "'\\''")
        escaped_destination = full_destination.replace("'", "'\\''")
        escaped_dest_parent = destination_dir.rstrip('/').replace("'", "'\\''")

        # Check if source directory exists and is a directory
        check_cmd = f"[ -d '{escaped_source}' ] && echo 'exists' || echo 'not exists'"
        result = ssh_command(check_cmd)
        if 'not exists' in result:
            error_message = f"Source directory does not exist or is not a directory: {full_source}"
            logging.error(error_message)
            print(error_message)
            return False

        # Create parent destination directory if it doesn't exist with proper permissions and ownership
        ssh_command(f"mkdir -p -m 775 '{escaped_dest_parent}' && "
                   f"chown {ssh_user}:{ssh_group} '{escaped_dest_parent}'")

        # Remove destination directory if it exists
        ssh_command(f"rm -rf '{escaped_destination}'")

        # Copy directory recursively (-r), preserve attributes (-p),
        # then set permissions and ownership recursively
        cp_cmd = (f"cp -rp '{escaped_source}' '{escaped_destination}' && "
                 # Set ownership recursively for all files and directories
                 f"chown -R {ssh_user}:{ssh_group} '{escaped_destination}' && "
                 # Set 775 permissions for all directories
                 f"find '{escaped_destination}' -type d -exec chmod 775 {{}} + && "
                 # Set 775 permissions for all files
                 f"find '{escaped_destination}' -type f -exec chmod 775 {{}} +")
        ssh_command(cp_cmd)

        return True

    except Exception as e:
        error_message = f"copy_dir error:\nsource: {full_source}\ndestination: {full_destination}\nerror: {str(e)}"
        logging.error(error_message)
        print(error_message)
        raise Exception(error_message)


def copy_dir_contents(
    source_dir: str,
    destination_dir: str,
    dir_name: str
) -> bool:
    """
    Copies contents of a directory recursively using cp command.
    If destination files exist, they will be overwritten.
    Sets 775 permissions and specified ownership for all copied files.

    :param source_dir: Base dir where the source directory is located
    :param destination_dir: Base dir where contents should be copied to
    :param dir_name: Name of the directory whose contents to copy
    :return: bool - True if successful, False if failed
    """
    try:
        full_source = f"{source_dir.rstrip('/')}/{dir_name}"
        escaped_source = full_source.replace("'", "'\\''")
        escaped_destination = destination_dir.rstrip('/').replace("'", "'\\''")

        # Check if source directory exists
        check_cmd = f"[ -d '{escaped_source}' ] && echo 'exists' || echo 'not exists'"
        if 'not exists' in ssh_command(check_cmd):
            error_message = f"Source directory does not exist: {full_source}"
            logging.error(error_message)
            print(error_message)
            return False

        # Create destination with permissions
        ssh_command(f"mkdir -p -m 775 '{escaped_destination}' && "
                   f"chown {ssh_user}:{ssh_group} '{escaped_destination}'")

        # Copy contents recursively preserving attributes
        cp_cmd = (f"cp -rp '{escaped_source}'/* '{escaped_destination}/' && "
                 f"chown -R {ssh_user}:{ssh_group} '{escaped_destination}' && "
                 f"find '{escaped_destination}' -type d -exec chmod 775 {{}} + && "
                 f"find '{escaped_destination}' -type f -exec chmod 775 {{}} +")
        ssh_command(cp_cmd)

        return True

    except Exception as e:
        error_message = f"copy_dir error:\nsource: {full_source}\ndestination: {destination_dir}\nerror: {str(e)}"
        logging.error(error_message)
        print(error_message)
        raise Exception(error_message)


def copy_file(
    source_dir: str,
    destination_dir: str,
    file_name: str
) -> bool:
    """
    Copies a single file on the remote machine using cp command and sets permissions/ownership.
    :param source_dir: Base dir where the source file is located
    :param destination_dir: Base dir where the file should be copied to
    :param file_name: Name of the file to copy
    :return: bool - True if successful, False if failed
    """
    try:
        # Construct full paths, ensuring no trailing slashes
        full_source = f"{source_dir.rstrip('/')}/{file_name}"
        full_destination = f"{destination_dir.rstrip('/')}/{file_name}"

        # Escape single quotes in paths for shell commands
        escaped_source = full_source.replace("'", "'\\''")
        escaped_destination = full_destination.replace("'", "'\\''")
        escaped_dest_parent = destination_dir.rstrip('/').replace("'", "'\\''")

        # Create destination directory if it doesn't exist (775 permissions)
        # Also set ownership of the directory
        ssh_command(f"mkdir -p -m 775 '{escaped_dest_parent}' && " 
                   f"chown {ssh_user}:{ssh_group} '{escaped_dest_parent}'")

        # Check if source file exists and is a regular file
        check_cmd = f"test -f '{escaped_source}'"
        exit_code = ssh_command(check_cmd)
        if exit_code != 0:
            error_message = f"Source file does not exist or is not a regular file: {full_source}"
            logging.error(error_message)
            print(error_message)
            return False

        # Copy file, set permissions (775) and ownership
        cp_cmd = (f"cp '{escaped_source}' '{escaped_destination}' && "
                 f"chmod 775 '{escaped_destination}' && "
                 f"chown {ssh_user}:{ssh_group} '{escaped_destination}'")
        ssh_command(cp_cmd)

        return True

    except Exception as e:
        error_message = f"copy_file error:\nsource: {full_source}\ndestination: {full_destination}\nerror: {str(e)}"
        logging.error(error_message)
        print(error_message)
        raise Exception(error_message)


def delete_dir_or_file(
    remote_dir,
    dir_or_file_name
):
    """
    Deletes a file or directory (including all its contents) from remote server using Paramiko SFTP.

    :param remote_dir: Base dir where the file/directory is located
    :param dir_or_file_name: Name of the directory or file to delete
    :return: bool - True if successful, False if failed
    """
    ssh = None
    sftp = None

    try:
        # Get SSH client
        ssh = get_client()

        # Open SFTP session
        sftp = ssh.open_sftp()

        def remove_recursive(path):
            """
            Recursively remove a path (file or directory)
            :param path: Path to remove
            """
            # Get path attributes
            attr = sftp.stat(path)

            if stat.S_ISDIR(attr.st_mode):  # If it's a directory
                # First remove all contents
                for item in sftp.listdir_attr(path):
                    item_path = f"{path}/{item.filename}"
                    remove_recursive(item_path)
                # Then remove the empty directory
                sftp.rmdir(path)
            else:  # If it's a file or link
                sftp.remove(path)

        # Construct full path
        full_path = f"{remote_dir.rstrip('/')}/{dir_or_file_name}"

        # Perform the deletion
        remove_recursive(full_path)

        return True

    except Exception as e:
        error_message = f"delete_dir_or_file error: \n" + \
                        f"remote_dir: {remote_dir.rstrip('/')}/{dir_or_file_name} \n" + \
                        f"error: {str(e)}"
        logging.error(error_message)
        print(error_message)
        raise Exception(error_message)

    finally:
        if sftp:
            sftp.close()
        if ssh:
            ssh.close()


# ------------------------------------------------------------------------------
# functions that will be called directly by functions in the core dir
# ------------------------------------------------------------------------------

def move_movie(
    download_dir,
    movie_dir,
    dir_or_file_name
):
    """
    move movie from media-dump to movie directory
    :param download_dir: directory where the file is downloaded
    :param movie_dir: directory where the movie will be moved
    :param dir_or_file_name: only the name of the file itself
    :return:
    """
    # determine if item is a file or directory
    full_download_path = os.path.join(download_dir, dir_or_file_name)

    dir_or_file_attr = check_dir_or_file_exists(
        remote_path=full_download_path
    )

    # copy file or directory
    if dir_or_file_attr[0] == False:
        error_message = (
f"""move_movie error: dir or file does not exist 
download_dir: \"{download_dir}\" 
dir_or_file_name: \"{dir_or_file_name}\"
full_download_path: \"{full_download_path}\""""
        )
        logging.error(error_message)
        print(error_message)
        raise Exception(error_message)
    elif dir_or_file_attr[1] == 'file':
        copy_file(
            source_dir=download_dir,
            destination_dir=movie_dir,
            file_name=dir_or_file_name
        )
    elif dir_or_file_attr[1] == 'directory':
        copy_dir(
            source_dir=download_dir,
            destination_dir=movie_dir,
            dir_name=dir_or_file_name
        )


def move_tv_show(
    download_dir: str,
    tv_show_dir: str,
    dir_or_file_name: str,
    tv_show_name: str,
    release_year: int,
    season: int
):
    """
    move tv show from media-dump to tv show directory
    :param download_dir: directory where the file is downloaded
    :param tv_show_dir: directory where the tv show will be moved
    :param dir_or_file_name: name of the file
    :param tv_show_name: name of the tv show
    :param release_year: year the tv show was released
    :param season: season number
    """
    # determine if item is a file or directory
    full_download_path = os.path.join(download_dir, dir_or_file_name)

    dir_or_file_attr = check_dir_or_file_exists(
        remote_path=full_download_path
    )

    # raise error if file or directory does not exist
    if dir_or_file_attr[0] == False:
        error_message = (
f"""move_movie error: dir or file does not exist 
download_dir: \"{download_dir}\" 
dir_or_file_name: \"{dir_or_file_name}\"
full_download_path: \"{full_download_path}\""""
        )
        logging.error(error_message)
        print(error_message)
        raise Exception(error_message)

    # generate the full destination path
    tv_show_name_formatted = tv_show_name.replace(' ', '-').lower()
    tv_show_year_formatted = str(int(release_year))
    tv_show_season_formatted = str(int(season)).zfill(2)
    full_destination_dir = tv_show_dir + tv_show_name_formatted + "-" + tv_show_year_formatted + "/s" + tv_show_season_formatted

    # determine if destination path exists
    full_destination_path_attr = check_dir_or_file_exists(
        remote_path=full_destination_dir
    )

    # create the folder if it does not exist
    if full_destination_path_attr[0] == False:
        create_dir(remote_path=full_destination_dir)

    # copy file or directory
    if dir_or_file_attr[1] == 'file':
        copy_file(
            source_dir=download_dir,
            destination_dir=full_destination_dir,
            file_name=dir_or_file_name
        )
    elif dir_or_file_attr[1] == 'directory':
        copy_dir(
            source_dir=download_dir,
            destination_dir=full_destination_dir,
            dir_name=dir_or_file_name
        )


def move_tv_season(
    download_dir: str,
    tv_show_dir: str,
    dir_name: str,
    tv_show_name: str,
    release_year: int,
    season: int
):
    """
    move tv show from media-dump to tv show directory
    :param download_dir: directory where the file is downloaded
    :param tv_show_dir: directory where the tv show will be moved
    :param dir_or_file_name: name of the file
    :param tv_show_name: name of the tv show
    :param release_year: year the tv show was released
    :param season: season number
    """
    # determine if item is a file or directory
    full_download_path = os.path.join(download_dir, dir_name)

    dir_or_file_attr = check_dir_or_file_exists(
        remote_path=full_download_path
    )

    # raise error if file or directory does not exist
    if dir_or_file_attr[0] == False:
        error_message = (
f"""move_movie error: dir or file does not exist 
download_dir: \"{download_dir}\" 
dir_or_file_name: \"{dir_name}\"
full_download_path: \"{full_download_path}\""""
        )
        logging.error(error_message)
        print(error_message)
        raise Exception(error_message)

    # generate the full destination path
    tv_show_name_formatted = tv_show_name.replace(' ', '-').lower()
    tv_show_year_formatted = str(int(release_year))
    tv_show_season_formatted = str(int(season)).zfill(2)
    full_destination_dir = tv_show_dir + tv_show_name_formatted + "-" + tv_show_year_formatted + "/s" + tv_show_season_formatted

    # determine if destination path exists
    full_destination_path_attr = check_dir_or_file_exists(
        remote_path=full_destination_dir
    )

    # create the folder if it does not exist
    if full_destination_path_attr[0] == False:
        create_dir(remote_path=full_destination_dir)

    # copy directory contents
    copy_dir_contents(
        source_dir=download_dir,
        destination_dir=full_destination_dir,
        dir_name=dir_name
    )

# ------------------------------------------------------------------------------
# end of sshf.py
# ------------------------------------------------------------------------------