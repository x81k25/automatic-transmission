from re import search
import feedparser
import pandas as pd
import re
import json
from ssh_functions import sshf
from rpc_functions import rpcf


# ------------------------------------------------------------------------------
# initiate movie torrents
# ------------------------------------------------------------------------------

def initiate_yts_torrent():
    transmission_client = rpcf.get_transmission_client()

    # add a magnet link
    transmission_client.add_torrent(
        "magnet:?xt=urn:btih:52612E49D776F211B2A7ED03DAE2050414FFABED&dn=Saturday.Night.Live.S50E01.Jean.Smart.1080p.WEB.h264-EDITH%5BTGx%5D&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce"
    )

    # add torrent link
    transmission_client.add_torrent(
        "https://yts.mx/torrent/download/CE030276BAF2D07E66EB50E47D6D33400D09EAA8"
    )

    torrents = transmission_client.get_torrents()

    for torrent in torrents:
        print(f"Torrent Name: {torrent.name}")
        print(f"Status: {torrent.status}")  # Status: downloading, seeding, paused, etc.
        print(f"Progress: {torrent.progress:.2f}%")
        print(f"Downloaded: {torrent.downloaded_ever / (1024 * 1024):.2f} MB")
        print(f"Uploaded: {torrent.uploaded_ever / (1024 * 1024):.2f} MB")
        print(f"Ratio: {torrent.ratio:.2f}")
        print(f"Seeders: {torrent.peers_getting_from_us}")
        print(f"Leechers: {torrent.peers_sending_to_us}")
        print(f"hash: {torrent.info_hash}")
        print("-" * 40)


# ------------------------------------------------------------------------------
# initiate show torrents
# ------------------------------------------------------------------------------

def initiate_tv_shows():
    # Instantiate transmission client
    transmission_client = rpcf.get_transmission_client()

    # Read in tv_shows.csv
    tv_shows = pd.read_csv('./data/tv_shows.csv')

    # Iterate through each row and check the status
    for index, row in tv_shows.iterrows():
        if row['status'] == 'ingested':
            # Submit the magnet link to the transmission client
            try:
                transmission_client.add_torrent(row['magnet_link'])
                # If successful, change the status to downloading
                tv_shows.at[index, 'status'] = 'downloading'
                print(f"downloading: {row['raw_title']} with link {row['magnet_link']}")
            except Exception as e:
                print(f"failed to download: {row['raw_title']} with magnet link {row['magnet_link']}. Error: {e}")

    # Save the updated tv_shows DataFrame
    tv_shows.to_csv('./data/tv_shows.csv', index=False)

# ------------------------------------------------------------------------------
# testing
# ------------------------------------------------------------------------------

# initiate_tv_shows()