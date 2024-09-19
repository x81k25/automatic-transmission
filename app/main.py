import os
from dotenv import load_dotenv
import paramiko

# Load environment variables from .env file
load_dotenv()

# load environment variables for SSH connection details
hostname = os.getenv('server-ip')
username = os.getenv('server-username')
password = os.getenv('server-password')
port = 22  # Default SSH port

# Create an SSH client instance
client = paramiko.SSHClient()

# Load system host keys (optional, can be disabled for local testing)
client.load_system_host_keys()

# Automatically add new host keys to known_hosts
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

# transmission connection details
trans_name = os.getenv('transmission-username')
trans_pass = os.getenv('transmission-password')

def transmission_shift(
	command: 'uname -a'
):
	"""
	Execute a command on the server
	:param command: Command to execute
	:return:
	"""
	try:
		# Connect to the server
		client.connect(hostname, port=port, username=username, password=password)

		# Execute the command
		stdin, stdout, stderr = client.exec_command(command)

		# Read the output from the command
		output = stdout.read().decode()
		error = stderr.read().decode()

		if output:
			print(f"Command output:\n{output}")
		if error:
			print(f"Command error:\n{error}")

	finally:
		# Close the connection
		client.close()

def get_status(
	username,
	password
):
	"""
	call transmission-remote list and view respnose
	:param username: transmission username
	:param password: transmission password
	"""
	command = "transmission-remote --auth " + \
		username + ":" + password + " --list"

	print(command)

	transmission_shift(command)

get_status(trans_name, trans_pass)

def add_magnet(
    username,
    password,
    magnet_link
):
    """
    add toreent via magnet link
    :param username: transmission username
    :param password: transmission password
    :param magnet_link: url of magent link
    :return:
    """
    command = "transmission-remote --auth " + \
		username + ":" + password + " --add " + magnet_link

    print(command)

    transmission_shift(command)

add_magnet(
	trans_pass,
	trans_name,
	""
)

def add_torrent_link(
	username,
	password,
	torrent_link
):
	"""
	add torrent via torrent link
	:param username:
	:param password:
	:param torrent_link:
	:return:
	"""
    command = "transmission-remote --auth " + \
        username + ":" + password + " --add " + torrent_link

    print(command)

	transmission_shift(command)

add_torrent_link(
	trans_name,
	trans_pass,
	torrent_link="https://yts.mx/torrent/download/5CBFB61D7D9132DAEE4D8B6C22BD80EADB82A373"
)

def parse_list(
	username,
	password
):
	"""
	function to call the torrent list and than parse into JSON
	:param username:
	:param password:
	:return:
	"""

	get_status(trans_name, trans_pass)


def remove_torrent(
	username,
	password,
	torrent_id
):
	"""
	remove torrent from the list
	:param username:
	:param password:
	:param torrent_id:
	:return