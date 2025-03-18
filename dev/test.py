# ################################################################################
# # extracting elements from feeds
# ################################################################################
# import json
# import src.utils as utils
#
# # for movies
# with open('./dev/data_examples/yts_feed.json', 'r') as f:
#     yts_feed = json.load(f)
#
# entry = yts_feed['entries'][0]
#
# # element needed to get hash
# entry['links'][1]['href']
#
# entry['link']
#
# utils.extract_hash_from_direct_download_url(entry['links'][1]['href'])
#
# # from episode feed
# with open('./dev/data_examples/episode_feed.json', 'r') as f:
#     episode_feed = json.load(f)
#
# entry = episode_feed['entries'][0]
#
# entry['links'][0]['href']
#
# entry['link']
#
# entry['title']
#
# utils.extract_hash_from_magnet_link(entry['links'][0]['href'])
#
# ------------------------------------------------------------------------------
# test OMDB API
# ------------------------------------------------------------------------------

from dotenv import load_dotenv
import os
import requests

# Load environment variables from .env file
load_dotenv()

# omdb environment variables
omdb_base_url = os.getenv('OMDB_BASE_URL')
api_key = os.getenv('OMDB_API_KEY')

#if media_type == 'movie':
params = {
    't': media_item["movie_title"],
    'y': media_item["release_year"],
    'apikey': api_key  # Your OMDb API key
}

#elif media_type == 'tv_show':
params = {
    't': "This is the Tom Green Documentary",
    'apikey': api_key
}

response = requests.get(omdb_base_url, params=params)


# # ------------------------------------------------------------------------------
# # clear out all local test folders
# # operation performed locally
# # ------------------------------------------------------------------------------
# from src.utils import utils
#
# command = \
# """rm -rf /home/x81-test/downloads/complete/*"""
#
# utils.ssh_command(command)
#
# # ------------------------------------------------------------------------------
# # alter test data from _01_feed_items_tv_show.pkl
# # ------------------------------------------------------------------------------
#
# import pickle
# import pandas as pd
#
# with open('./test/data/_01_feed_items_tv_show.pkl', 'rb') as f:
#     feed_items = pickle.load(f)
#
# feed_items.loc[feed_items['tv_show_name'] == '60 Minutes (US)', 'tv_show_name'] = '60 Minutes'
#
# # save feed items back to file
# with open('./test/data/_01_feed_items_tv_show.pkl', 'wb') as f:
#     pickle.dump(feed_items, f)
#
# # ------------------------------------------------------------------------------
# # fix 60 minutes metadata collection error
# # ------------------------------------------------------------------------------
#
# import os
# import requests
# from dotenv import load_dotenv
#
# load_dotenv()
#
# api_key = os.getenv('OMDB_API_KEY')
# omdb_base_url = os.getenv('OMDB_BASE_URL')
#
# params = {
#     't': '60 Minutes',
#     'apikey': api_key  #
# }
#
# # Make a request to the OMDb API
# response = requests.get(omdb_base_url, params=params)
#
# # ------------------------------------------------------------------------------
# # debug transmission rpc
# # ------------------------------------------------------------------------------
#
# import os
# from dotenv import load_dotenv
# from transmission_rpc import Client as Transmission_client
#
# # Load environment variables from .env file
# load_dotenv()
#
# # transmission connection details
# hostname=os.getenv('SERVER_IP')
# transmission_port = 9091
# transmission_username = os.getenv('TRANSMISSION_USERNAME')
# transmission_password = os.getenv('TRANSMISSION_PASSWORD')
#
# print(f"Username: {transmission_username}")
# print(f"Password: {transmission_password}")
#
# client = Transmission_client(
#     host=hostname,
#     port=transmission_port,
#     username=transmission_username,
#     password=transmission_password
# )
#
#
# # ------------------------------------------------------------------------------
# # create movie data frame that will be used throughout pipeline
# # ------------------------------------------------------------------------------
# from src.utils import rpcf, sshf
# import pickle
# import pandas as pd
# import os
# from dotenv import load_dotenv
#
# load_dotenv()
#
# def restart_movies():
#
#     # define data types for all values of movies
#     movies = pd.DataFrame({
#         'hash': pd.Series([], dtype='string'),
#         'movie_title': pd.Series([], dtype='string'),
#         'release_year': pd.Series([], dtype='Int64'),
#         'status': pd.Series([], dtype='string'),
#         'torrent_source': pd.Series([], dtype='string'),
#         'rejection_reason': pd.Series([], dtype='string'),
#         'published_timestamp': pd.Series([], dtype='string'),
#         'summary': pd.Series([], dtype='string'),
#         'genre': pd.Series([], dtype='object'),
#         'language': pd.Series([], dtype='object'),
#         'metascore': pd.Series([], dtype='Int64'),
#         'rt_score': pd.Series([], dtype='Int64'),
#         'imdb_rating': pd.Series([], dtype='Float64'),
#         'imdb_votes': pd.Series([], dtype='Int64'),
#         'imdb_id': pd.Series([], dtype='string'),
#         'resolution': pd.Series([], dtype='string'),
#         'video_codec': pd.Series([], dtype='string'),
#         'upload_type': pd.Series([], dtype='string'),
#         'audio_codec': pd.Series([], dtype='string'),
#         'raw_title': pd.Series([], dtype='string'),
#         'file_name': pd.Series([], dtype='string'),
#         'uploader': pd.Series([], dtype='string')
#     })
#
#     movies.set_index('hash', inplace=True)
#
#     with open('../data/movies.pkl', 'wb') as file:
#         pickle.dump(movies, file)
#
#     # purge current torrent queue
#     rpcf.purge_torrent_queue()
#
#     # purge media dump folder
#     try:
#         sshf.purge_media_dump()
#     except:
#         pass
#
#     # create test tv folder
#     movie_dir = os.getenv('movie_dir')
#     sshf.create_dir(movie_dir)
#     # create test tv folder
#     tv_show_dir = os.getenv('tv_show_dir')
#     sshf.create_dir(tv_show_dir)
#
# restart_movies()
#
# # ------------------------------------------------------------------------------
# # read in movies data frame
# # ------------------------------------------------------------------------------
# import pickle
#
# with open('../data/movies.pkl', 'rb') as file:
#     movies = pickle.load(file)
#
# # print all dtypes in movies
# print(movies.dtypes)
#
# # ------------------------------------------------------------------------------
# # manually adjust status of movies
# # ------------------------------------------------------------------------------
# import pickle
#
# with open('../data/movies.pkl', 'rb') as file:
#     movies = pickle.load(file)
#
# for row in movies.iterrows():
#     if movies.at[row[0], 'status'] == 'complete':
#         movies.at[row[0], 'status'] = 'queued'
#
# with open('../data/movies.pkl', 'wb') as file:
#     pickle.dump(movies, file)
#
# # ------------------------------------------------------------------------------
# # create tv data frame that will be used throughout pipeline
# # ------------------------------------------------------------------------------
# from src.utils import  rpcf, sshf
# import pickle
# import pandas as pd
# from dotenv import load_dotenv
# import os
#
# load_dotenv()
#
# def restart_tv():
#     # sepicfy dtype for every element in the tv_shows
#     tv_shows = pd.DataFrame({
#         "hash": pd.Series([], dtype="string"),
#         "tv_show_name": pd.Series([], dtype="string"),
#         "season": pd.Series([], dtype="int32"),
#         "episode": pd.Series([], dtype="int32"),
#         "status": pd.Series([], dtype="string"),
#         "torrent_source": pd.Series([], dtype="string"),
#         "published_timestamp": pd.Series([], dtype="string"),
#         "rejection_reason": pd.Series([], dtype="string"),
#         "summary": pd.Series([], dtype="string"),
#         "release_year": pd.Series([], dtype="int32"),
#         "genre": pd.Series([], dtype="object"),
#         "language": pd.Series([], dtype="object"),
#         "metascore": pd.Series([], dtype="int32"),
#         "imdb_rating": pd.Series([], dtype="float32"),
#         "imdb_votes": pd.Series([], dtype="int32"),
#         "imdb_id": pd.Series([], dtype="string"),
#         "resolution": pd.Series([], dtype="string"),
#         "video_codec": pd.Series([], dtype="string"),
#         "upload_type": pd.Series([], dtype="string"),
#         "audio_codec": pd.Series([], dtype="string"),
#         "raw_title": pd.Series([], dtype="string"),
#         "file_name": pd.Series([], dtype="string")
#     })
#
#     tv_shows.set_index('hash', inplace=True)
#
#     with open('../data/tv_shows.pkl', 'wb') as file:
#         pickle.dump(tv_shows, file)
#
#     # purge current torrent queue
#     rpcf.purge_torrent_queue()
#
#     # purge media dump folder
#     try:
#         sshf.purge_media_dump()
#     except:
#         pass
#
#     # create test tv folder
#     movie_dir = os.getenv('movie_dir')
#     sshf.create_dir(movie_dir)
#     # create test tv folder
#     tv_show_dir = os.getenv('tv_show_dir')
#     sshf.create_dir(tv_show_dir)
#
# restart_tv()
#
# # ------------------------------------------------------------------------------
# # read in tv_shows.pkl
# # ------------------------------------------------------------------------------
# import pickle
#
# with open('../data/tv_shows.pkl', 'rb') as file:
#     tv_shows = pickle.load(file)
#
# # ------------------------------------------------------------------------------
# # manually adjust status of tv_shows
# # ------------------------------------------------------------------------------
#
# import pickle
#
# with open('../data/tv_shows.pkl', 'rb') as file:
#     tv_shows = pickle.load(file)
#
# old_status = 'downloaded'
# new_status = 'downloading'
#
# for index in tv_shows.index:
#     if tv_shows.loc[index, 'status'] == old_status:
#         tv_shows.loc[index, 'status'] = new_status
#
# with open('../data/tv_shows.pkl', 'wb') as file:
#     pickle.dump(tv_shows, file)
#
# # ------------------------------------------------------------------------------
# # manually add movie
# # ------------------------------------------------------------------------------
# import os
# from src.utils import rpcf, sshf
# import pickle
# from dotenv import load_dotenv
#
# load_dotenv()
#
# # remove torrent from transmission given index
# transmission_client = rpcf.get_transmission_client()
#
# # get all torrent objects from transmission and print the file name and index
# torrents = transmission_client.get_torrents()
# for index, torrent in enumerate(torrents):
#     print(f"Index: {torrent.info_hash}")
#     print(f"Name: {torrent.name}")
#     print("-" * 40)
#
# transmission_client.remove_torrent("abaa10da30f669c586d88c14f88aa51a0add1799")
#
# # print contents of media dump
# print(sshf.print_dump_contents())
#
# ssh command output: total 20
# drwxrwxr-x 2 debian-transmission debian-transmission 4096 Dec 31 16:02 Its Always Sunny in Philadelphia S16E01 The Gang Inflates 1080p AMZN WEB-DL DDP5 1 H 264-FLUX[TGx]
# drwxrwxr-x 2 debian-transmission debian-transmission 4096 Dec 31 15:27
# drwxrwxr-x 3 debian-transmission debian-transmission 4096 Dec 31 15:26
# drwxrwxr-x 3 debian-transmission debian-transmission 4096 Dec 31 15:28
# drwxrwxr-x 3 debian-transmission debian-transmission 4096 Dec 31 15:28
#
# # move movie
# sshf.move_movie(
#     dir_or_file_name = 'Megalopolis (2024) [1080p] [WEBRip] [x265] [10bit] [5.1] [YTS.MX]',
#     download_dir = os.getenv('DOWNLOAD_DIR'),
#     movie_dir = "/k/media/video/movies/"
# )
#
# # ------------------------------------------------------------------------------
# # manually add tv show
# # ------------------------------------------------------------------------------
# from src.utils import rpcf, sshf
# import os
# from dotenv import load_dotenv
#
# # remove torrent from transmission given index
# transmission_client = rpcf.get_transmission_client()
#
# load_dotenv()
#
# # get the file names and status of all active torrent from transmission
# torrents = transmission_client.get_torrents()
# for torrent in torrents:
#     print(f"Torrent Name: {torrent.name}")
#     print(f"Status: {torrent.status}")  # Status: downloading, seeding, paused, etc.
#     print(f"Progress: {torrent.progress:.2f}%")
#     print(f"Ratio: {torrent.ratio:.2f}")
#     print(f"Hash: {torrent.info_hash}")
#     print(f"ETA: {torrent.eta}")
#     print("-" * 40)
#
# # print contents of media dump
# print(sshf.print_dump_contents())
#
# # move tv show
# sshf.move_tv_show(
#     dir_or_file_name = 'Dune Prophecy S01E05 1080p x265-ELiTE[EZTVx.to].mkv',
#     tv_show_name = 'dune-prophecy',
#     release_year = 2024,
#     season = 1,
#     DOWNLOAD_DIR = os.getenv('DOWNLOAD_DIR'),
#     tv_show_dir = "/k/media/video/tv/"
# )
#
# # ------------------------------------------------------------------------------
# # manually filter torrent
# # ------------------------------------------------------------------------------
# import pickle
# import src.core._05_filter as ti
# import pandas as pd
#
# with open('../data/movies.pkl', 'rb') as file:
#     movies = pickle.load(file)
#
# movie = movies.iloc[3]
# #print(movie["raw_title"])
# #print(movie["rejection_reason"])
# #print(movie["resolution"])
# #print(movie["language"])
# #print(movie["rt_score"])
#
# for index, row in movies.iterrows():
#     print(ti.filter_item(item = row, filter_type="movie"))
#
#
# # ------------------------------------------------------------------------------
# # test ssh functions
# # ------------------------------------------------------------------------------
# from src.utils import ssh_command
#
# command = 'cp -rf "/k/media/media-dump/Last Week Tonight with John Oliver S11E22 1080p WEB H264 Success" "/k/media/media-dump/test/tv/last-week-tonight-with-john-oliver-2014/s11/"'
#
# print(ssh_command(command))
#
#
#
# # ------------------------------------------------------------------------------
# # test attributes of a given torrent
# # ------------------------------------------------------------------------------
# import pickle
# from src.utils import rpcf
#
# with open('../data/tv_shows.pkl', 'rb') as file:
#     tv_shows = pickle.load(file)
#
# tv_show = tv_shows.loc[tv_shows.index[-2]]
#
# tv_show.tv_show_name
# tv_show.name
#
# torrent = transmission_client.get_torrent(tv_show.name)
#
# torrent.name
#
# tv_show2 = tv_show.copy()
#
# tv_show2.file_name
#
# tv_show2.file_name = torrent.name
#
# tv_show2.file_name
#
#
#
# # ------------------------------------------------------------------------------
# # test methods and functions
# # ------------------------------------------------------------------------------
#
# from src.utils import rpcf
# from src.utils import sshf
#
# transmission_client = rpcf.get_transmission_client()
#
# # get the status of the transmission service
# sshf.get_transmission_service_status()
#
# # get the status of individual torrents
# torrents = transmission_client.get_torrents()
# torrent = torrents[0]
# torrent.id
# torrent.info_hash
#
# for torrent in torrents:
#     print(f"Torrent Name: {torrent.name}")
#     print(f"Status: {torrent.status}")  # Status: downloading, seeding, paused, etc.
#     print(f"Progress: {torrent.progress:.2f}%")
#     print(f"Downloaded: {torrent.downloaded_ever / (1024 * 1024):.2f} MB")
#     print(f"Uploaded: {torrent.uploaded_ever / (1024 * 1024):.2f} MB")
#     print(f"Ratio: {torrent.ratio:.2f}")
#     print(f"Seeders: {torrent.peers_getting_from_us}")
#     print(f"Leechers: {torrent.peers_sending_to_us}")
#     print(f"hash: {torrent.info_hash}")
#     print("-" * 40)
#
#
#
# sshf.ssh_command("ls -l")
# sshf.ssh_command('if [-d "/k/media/media-dump/test/tv/sesame-street-1969/s54/"]; then echo "True"; else echo "false"; fi')
# print(sshf.ssh_command("ls -l"))
#
# sshf.trans_stop()
#
# sshf.trans_start()
#
# sshf.trans_restart()
#
# test_string = "command output: false     "
#
# str(int(50.12345))
#
# # ------------------------------------------------------------------------------
# # save object for testing
# # ------------------------------------------------------------------------------
# import pickle
#
# with open('./test/data/_01_feed_tv_show.pkl', 'wb') as f:
#     pickle.dump(feed, f)
#
# with open('./test/data/_01_rss_url_tv_show.pkl', 'wb') as f:
#     pickle.dump(rss_url, f)
