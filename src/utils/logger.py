import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
log_dir = os.getenv('log_dir')


def logger(input_string=""):
	"""
	write a function that accepts strings arguments and writes them to a file
	:param input_string: string to write to file
	:return:
	"""
	# get current timestamp and format as [YYYY-MM-DD HH:MM:SS]
	current_timestamp = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

	# open file in append mode
	with open(log_dir, "a") as file:
		# write timestamp and input_string to file
		file.write(f"{current_timestamp} {input_string}\n")