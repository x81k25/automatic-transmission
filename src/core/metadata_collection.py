from dotenv import load_dotenv
import pandas as pd
import pickle
import os
import re
import requests
from src.utils import logger

# ------------------------------------------------------------------------------
# load environment variables and
# ------------------------------------------------------------------------------

# Load environment variables from .env file
load_dotenv()

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

def get_tv_show_omdb_metadata(tv_show):

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

    data = query_omdb_api(tv_show['tv_show_name'])
    if data and data['Response'] == 'True':
        tv_show['release_year'] = data.get('Year', None)
        tv_show['release_year'] = re.sub(r'\D', '', tv_show['release_year'])
        tv_show['genre'] = data.get('Genre', None)
        tv_show['language'] = data.get('Language', None)
        tv_show['metascore'] = data.get('Metascore', None)
        tv_show['imdb_rating'] = data.get('imdbRating', None)
        if tv_show['imdb_rating'] != "N/A":
            tv_show['imdb_rating'] = float(re.sub(r"\D", "", tv_show['imdb_rating']))
        tv_show['imdb_votes'] = data.get('imdbVotes', None)
        if tv_show['imdb_rating'] != "N/A":
            tv_show['imdb_votes'] = int(re.sub(r"\D", "", tv_show['imdb_votes']))
        tv_show['imdb_id'] = data.get('imdbID', None)

    # Save the updated tv_shows DataFrame
    return tv_show

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
            logger(f"Error fetching data for {movie_name}: {movie_data}")

    movie_data_df = pd.DataFrame(movie_data_list)
    merged_df = pd.merge(feed_df, movie_data_df, left_on='movie_title',
                         right_on='Title', how='left')

    return merged_df


# ------------------------------------------------------------------------------
# main metadata collection showRSS pipeline
# ------------------------------------------------------------------------------

def get_tv_show_metadata():
    # red in tv_shows.pkl
    with open('./data/tv_shows.pkl', 'rb') as file:
        tv_shows = pickle.load(file)

    # for each row of tv_shows pass the individual row to get_tv_show_omdb_metadata and replace all updated files in the original data frame
    tv_shows = tv_shows.apply(
        lambda row: get_tv_show_omdb_metadata(row) if row["status"] == "ingested" else row,
        axis=1
    )

    # iterate through each column with the status of ingested, if the value of release year is populated, then the status is updated to queued and print an update
    for index in tv_shows.index:
        if tv_shows.loc[index, 'status'] == 'ingested' and tv_shows.loc[index, 'release_year'] is not None:
            tv_shows.loc[index, 'status'] = 'queued'
            logger(f"queued: {tv_shows.loc[index, 'raw_title']} with hash {index}")

    # write tv shows back to csv
    with open('./data/tv_shows.pkl', 'wb') as file:
        pickle.dump(tv_shows, file)

# ------------------------------------------------------------------------------
# test
# ------------------------------------------------------------------------------

#get_tv_show_metadata()

# get_tv_show