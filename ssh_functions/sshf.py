import os
from dotenv import load_dotenv
import paramiko

# ------------------------------------------------------------------------------
# load environment variables and
# ------------------------------------------------------------------------------

# Load environment variables from .env file
load_dotenv()

# load environment variables for SSH connection details
hostname = os.getenv('server-ip')
ssh_username = os.getenv('server-username')
ssh_password = os.getenv('server-password')
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
			username=ssh_username,
			password=ssh_password
		)

		# Execute the command
		stdin, stdout, stderr = ssh_client.exec_command(command)

		# Read the output from the command
		output = stdout.read().decode()
		error = stderr.read().decode()

		if output:
			print(f"Command output:\n{output}")
		if error:
			print(f"Command error:\n{error}")

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
	ssh_command('echo "7869" | sudo -S systemctl start transmission-daemon')


def trans_stop():
	"""
	stop the transmission service
	:return:
	"""
	ssh_command('echo "7869" | sudo -S systemctl stop transmission-daemon')


def trans_restart():
	"""
	restart the transmission service
	:return:
	"""
	ssh_command('echo "7869" | sudo -S systemctl restart transmission-daemon')


def move_movie(file_name):
	"""
	move movie from media-dump to movie directory
	:param file_name: only the name of the file itself
	:return:
	"""
	command = 'echo "7869" | sudo -S mv ' + \
        '"/k/media/media-dump/' + file_name + '" ' + \
	    "/k/media/video/movies"

	print(command)

	ssh_command(command)


def move_tv_show(
	file_name,
	show,
	season
):
	"""
	move tv show from media-dump to tv show directory
	:param file_name: only the name of the file itself
	:param show: name of the show in the format "the-show-2000"
	:param season: season number in the format "s01"
	"""
	command = 'echo "7869" | sudo -S mv ' + \
		'"/k/media/media-dump/' + file_name + '" ' + \
		"/k/media/video/tv/" + show + "/" + season

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
	command = 'echo "7869" | sudo -S rm -r /k/media/media-dump/*'

	ssh_command(command)