import os
from dotenv import load_dotenv
from transmission_rpc import Client as Transmission_client
import pandas as pd
from ssh_functions import sshf
from rpc_functions import rpcf
from tmdbv3api import TMDb, Movie
import json
import requests

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

# transmission connection details
transmission_username = os.getenv('transmission-username')
transmission_password = os.getenv('transmission-password')
transmisson_port = 9091

# base URL for OMDb API
BASE_URL = 'http://www.omdbapi.com/'

# api key for OMDb API
api_key = os.getenv('omdb_api_key')

# ------------------------------------------------------------------------------
# metadata collection helper functions
# ------------------------------------------------------------------------------

# read in feed_df.csv and store as dataframe
def read_feed_df():
	"""
	read in feed_df.csv and store as dataframe
	:return: dataframe
	"""
	feed_df = pd.read_csv('./feed_df.csv')
	return feed_df


def get_movie_data(movie_name, year=None):
	# Create query parameters
	params = {
		't': movie_name,  # 't' stands for title
		'apikey': api_key
	}

	# Add the year to the query if provided
	if year:
		params['y'] = year

	# Make a request to the OMDb API
	response = requests.get(BASE_URL, params=params)

	# Check if the request was successful
	if response.status_code == 200:
		# Convert the response to JSON format
		data = response.json()

		# Check if the movie was found
		if data['Response'] == 'True':
			return data
		else:
			return f"Error: {data['Error']}"
	else:
		return f"Error: Unable to connect to OMDb API. Status code: {response.status_code}"

def get_show_release_year(title, api_key):
	# Define the base URL for OMDb API
	base_url = "http://www.omdbapi.com/"

	# Parameters to be sent to the API
	params = {
		"t": title,  # 't' is for title
		"type": "series",  # Specify that we are looking for a TV show
		"apikey": api_key  # Your OMDb API key
	}

	# Send a GET request to the API
	response = requests.get(base_url, params=params)

	# Convert the response to JSON format
	data = response.json()

	# Check if the request was successful and return the year
	if response.status_code == 200 and "Year" in data:
		return data["Year"]
	else:
		return "Show not found or invalid API key."


# ------------------------------------------------------------------------------
# main metadata collection function
# ------------------------------------------------------------------------------


def collect_metadata(feed_df):
	"""
	Loop through all of the movie titles and release years in feed_df,
	call the OMDb API to get the movie data, save the data into a data frame,
	and merge it with the existing feed_df.
	"""
	movie_data_list = []

	for index, row in feed_df.iterrows():
		movie_name = row['movie_title']
		release_year = row.get('release_year', None)
		movie_data = get_movie_data(movie_name, release_year)

		if isinstance(movie_data, dict):
			movie_data_list.append(movie_data)
		else:
			print(f"Error fetching data for {movie_name}: {movie_data}")

	movie_data_df = pd.DataFrame(movie_data_list)
	merged_df = pd.merge(feed_df, movie_data_df, left_on='movie_title',
						 right_on='Title', how='left')

	return merged_df


