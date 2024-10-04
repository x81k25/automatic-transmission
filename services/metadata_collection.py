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


def get_movie_omdb_data(movie_name, year=None):
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

def get_tv_show_omdb_metadata(tv_shows):

    #subset tv shows to include only shows marked as ingested
    tv_shows_ingested = tv_shows[tv_shows['status'] == 'ingested']

    # Function to query OMDb API
    def query_omdb_api(show_title):
        params = {
            't': show_title,
            'apikey': api_key
        }
        response = requests.get(BASE_URL, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            return None

    # Iterate through each row and update metadata
    for index, row in tv_shows_ingested.iterrows():
        tv_show_name = row['tv_show_name']
        data = query_omdb_api(tv_show_name)
        if data and data['Response'] == 'True':
            tv_shows_ingested.at[index, 'release_year'] = data.get('Year', None).lstrip('–')
            tv_shows_ingested.at[index, 'genre'] = data.get('Genre', None)
            tv_shows_ingested.at[index, 'language'] = data.get('Language', None)
            tv_shows_ingested.at[index, 'metascore'] = data.get('Metascore', None)
            tv_shows_ingested.at[index, 'imdb_rating'] = data.get('imdbRating', None)
            tv_shows_ingested.at[index, 'imdb_votes'] = data.get('imdbVotes', None)
            tv_shows_ingested.at[index, 'imdb_id'] = data.get('imdbID', None)
            # change status of each element to "queued" if API call successful
            tv_shows_ingested.at[index, 'status'] = 'queued'
            # output print the status change
            print(f"queued: {row['raw_title']} with link {row['magnet_link']}")
        else:
            print(f"failed to queue: {row['raw_title']} with link {row['magnet_link']}")

    # Set the index to 'hash' for both DataFrames
    tv_shows_ingested.set_index('hash', inplace=True)
    tv_shows.set_index('hash', inplace=True)

    # Combine the DataFrames, replacing rows in tv_shows with those in tv_shows_ingested
    tv_shows.update(tv_shows_ingested)

    # Reset the index
    tv_shows.reset_index(inplace=True)

    # Save the updated tv_shows DataFrame
    return tv_shows

# ------------------------------------------------------------------------------
# main metadata collection yts pipeline
# ------------------------------------------------------------------------------


def collect_movie_metadata(feed_df):
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


# ------------------------------------------------------------------------------
# main metadata collection showRSS pipeline
# ------------------------------------------------------------------------------

def get_tv_show_metadata():
    # red in tv_shows.csv
    tv_shows = pd.read_csv('./data/tv_shows.csv')

    # get omdb for each show
    tv_shows = get_tv_show_omdb_metadata(tv_shows)

    # write tv shows back to csv
    tv_shows.to_csv('./data/tv_shows.csv', index=False)


