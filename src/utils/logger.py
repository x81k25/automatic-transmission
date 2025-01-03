import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
log_dir = os.getenv('LOG_DIR')


def log(input_string=""):
	"""
	write a function that accepts strings arguments and writes them to a file
	:param input_string: string to write to file
	:return:
	"""
	# get current timestamp and format as [YYYY-MM-DD HH:MM:SS]
	current_timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

	# create log output
	log_output = f"{current_timestamp} {input_string}\n"

	# print with log formatting
	print(log_output)