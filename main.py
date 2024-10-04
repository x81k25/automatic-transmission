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

def tv_show_pipeline():
	rss_ingest.tv_show_full_ingest()()

	metadata_collection.get_tv_show_metadata()

	torrent_initiation.initiate_tv_shows()







