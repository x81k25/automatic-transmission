import pandas as pd

from ssh_functions import sshf
from rpc_functions import rpcf

from services import rss_ingest
from services import metadata_collection
from services import torrent_initiation
from services import torrent_cleanup

# ------------------------------------------------------------------------------
# full movie pipeline
# ------------------------------------------------------------------------------


# ------------------------------------------------------------------------------
# full tv show pipeline
# ------------------------------------------------------------------------------

rss_ingest.tv_full_ingest()

torrent_initiation.initiate_tv_shows()






# ------------------------------------------------------------------------------
# create data frames that will be used throughout pipeline
# ------------------------------------------------------------------------------

def restart_tv():
    # purge current torrent queue
    rpcf.purge_torrent_queue()

    # purge media dump folder
    sshf.purge_media_dump()

    # create data frame for tv shows
    tv_columns = [
        "hash",
        "tv_show_name",
        "season",
        "episode",
        "status",
        "magnet_link",
        "published_timestamp",
        "summary",
        "resolution",
        "video_codec",
        "upload_type",
        "audio_codec",
        "raw_title",
        "feed_id",
        "tv_show_id",
        "tv_episode_id",
        "tv_external_id"
    ]

    tv_shows = pd.DataFrame(columns=tv_columns)

    tv_shows.to_csv('./data/tv_shows.csv', index=False)

#restart_tv()

# ------------------------------------------------------------------------------
# manually add tv show
# ------------------------------------------------------------------------------

transmission_client = rpcf.get_transmission_client()

# add a magnet link
transmission_client.add_torrent(
	"magnet:?xt=urn:btih:985A032D82D0D075873710F8253D14B80B876753&dn=The%20Penguin%20S01E02%201080p%20WEB%20H264-SuccessfulCrab&tr=udp%3A%2F%2Ftracker.opentrackr.org%3A1337&tr=udp%3A%2F%2Fopen.stealth.si%3A80%2Fannounce&tr=udp%3A%2F%2Ftracker.torrent.eu.org%3A451%2Fannounce&tr=udp%3A%2F%2Ftracker.bittor.pw%3A1337%2Fannounce&tr=udp%3A%2F%2Fpublic.popcorn-tracker.org%3A6969%2Fannounce&tr=udp%3A%2F%2Ftracker.dler.org%3A6969%2Fannounce&tr=udp%3A%2F%2Fexodus.desync.com%3A6969&tr=udp%3A%2F%2Fopen.demonii.com%3A1337%2Fannounce"
)

# add torrent link
transmission_client.add_torrent(
	"https://yts.mx/torrent/download/CE4CD1E48C24FAF6D41481ADB47973DD8C5189FF"
)

# get the file names and status of all active torrent from transmission
torrents = transmission_client.get_torrents()
for torrent in torrents:
    print(f"Torrent Name: {torrent.name}")
    print(f"Status: {torrent.status}")  # Status: downloading, seeding, paused, etc.
    print(f"Progress: {torrent.progress:.2f}%")
    print(f"Ratio: {torrent.ratio:.2f}")
    print(f"Hash: {torrent.info_hash}")
    print(f"ETA: {torrent.eta}")
    print("-" * 40)

# print contents of media dump
sshf.print_dump_contents()

# move tv show
move_tv_show(
	file_name="" ,
	show="the-penguin-2024",
	season="s01"
)

# ------------------------------------------------------------------------------
# test methods and functions
# ------------------------------------------------------------------------------

transmission_client = rpcf.get_transmission_client()

# get the status of the transmission service
sshf.get_transmission_service_status()

# get the status of individual torrents
torrents = transmission_client.get_torrents()
torrent = torrents[0]
torrent.id
torrent.info_hash

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





trans_stop()

trans_start()

trans_restart()












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


