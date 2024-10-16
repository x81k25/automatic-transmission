import os
from dotenv import load_dotenv
import paramiko
from src.utils import logger

# ------------------------------------------------------------------------------
# load environment variables and
# ------------------------------------------------------------------------------

# Load environment variables from .env file
load_dotenv()

# load environment variables for SSH connection details
hostname = os.getenv('server-ip')
ssh_user = os.getenv('media-user')
ssh_password = os.getenv('media-password')
ssh_group = os.getenv('media-group')
ssh_port = 22  # Default SSH port

# ------------------------------------------------------------------------------
# functions that communicate directly with the CLI
# ------------------------------------------------------------------------------

def ssh_command(command: 'uname -a'):
    """
    Execute a command on the server
    :param command: Command to execute
    :return:
    """
    # instantiate client object
    ssh_client = paramiko.SSHClient()

    # Load system host keys (optional, can be disabled for local testing)
    ssh_client.load_system_host_keys()

    # Automatically add new host keys to known_hosts
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Connect to the server
        ssh_client.connect(
            hostname=hostname,
            port=ssh_port,
            username=ssh_user,
            password=ssh_password
        )

        # Execute the command
        stdin, stdout, stderr = ssh_client.exec_command(command)

        # Read the output from the command
        output = stdout.read().decode()
        error = stderr.read().decode()

        if output:
            return f"command output: {output}"
        if error:
            logger.logger(f"command error: {error}")
            return f"command error: {error}"

    finally:
        # Close the connection
        ssh_client.close()

def get_transmission_service_status():
    """
    get the status of the transmission service
    :return:
    """
    ssh_command('systemctl status transmission-daemon')

def trans_start():
    """
    start the transmission service
    :return:
    """
    ssh_command('echo \"' + ssh_password + '\" | sudo -S systemctl start transmission-daemon')


def trans_stop():
    """
    stop the transmission service
    :return:
    """
    ssh_command('echo \"' + ssh_password + '\" | sudo -S systemctl stop transmission-daemon')


def trans_restart():
    """
    restart the transmission service
    :return:
    """
    ssh_command('echo \"' + ssh_password + '\" | sudo -S systemctl restart transmission-daemon')


def move_movie(
    file_name,
    download_dir,
    movie_dir
):
    """
    move movie from media-dump to movie directory
    :param file_name: only the name of the file itself
    :return:
    """
    # set proper permissions to file
    command = 'chown -R ' + ssh_user + ':' + ssh_group + ' ' + \
        download_dir + file_name

    ssh_command(command)

    # move file
    command = 'mv ' + download_dir + file_name + ' ' + movie_dir

    ssh_command(command)


def move_tv_show(
    download_dir,
    tv_show_dir,
    file_name,
    tv_show_name,
    release_year,
    season
):
    """
    move tv show from media-dump to tv show directory
    :param download_dir: directory where the file is downloaded
    :param tv_show_dir: directory where the tv show will be moved
    :param file_name: name of the file
    :param tv_show_name: name of the tv show
    :param release_year: year the tv show was released
    :param season: season number
    """
    # generate full path for current file location
    full_download_path = '\"' + download_dir + file_name + '\"'

    # generate full path for file destination
    tv_show_name_formatted = tv_show_name.replace(' ', '-').lower()
    tv_show_year_formatted = str(int(release_year))
    tv_show_season_formatted = str(int(season)).zfill(2)
    full_destination_path = '\"' + tv_show_dir + tv_show_name_formatted + "-" + tv_show_year_formatted + "/s" + tv_show_season_formatted + '/\"'

    # determine if desired destination directory exists
    command =  'if [-d ' + full_destination_path + ']; then echo \"True\"; ' + \
        'else echo \"False"; fi'

    dir_exists = ssh_command(command)
    dir_exists = dir_exists.lstrip("command output: ").strip()

    if dir_exists == "False":
        # create directory if it does not exist
        command = 'mkdir -p ' + full_destination_path
        ssh_command(command)
    elif dir_exists == "True":
        pass
    else:
        logger("error: could not determine if " + full_destination_path + " exists")

    # copy file
    command = 'cp -rf ' + full_download_path + ' ' + full_destination_path + ''

    ssh_command(command)

    # delete original file
    command = 'rm -r ' + full_download_path

    ssh_command(command)


def print_dump_contents():
    """
    print the contents of the media-dump directory
    :return: contents
    """
    command = "ls -l /k/media/media-dump "

    response = ssh_command(command)

    return response


def purge_media_dump():
    """
    remove all files from the media-dump directory
    :return:
    """
    command = 'rm -r /k/media/media-dump/*'

    ssh_command(command)