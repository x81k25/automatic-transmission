# standard library imports
import argparse
import logging

# local/custom imports
import src.core as core

# ------------------------------------------------------------------------------
# setup
# ------------------------------------------------------------------------------

def setup_logging(debug=False):
    # Define custom VERBOSE level (set to 5, below DEBUG)
    VERBOSE = 5
    logging.addLevelName(VERBOSE, "VERBOSE")
    
    # Add verbose method to Logger class
    def verbose(self, message, *args, **kwargs):
        if self.isEnabledFor(VERBOSE):
            self._log(VERBOSE, message, args, **kwargs)
    
    # Add the method to the Logger class
    logging.Logger.verbose = verbose
    
    if not debug:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
        logging.getLogger("paramiko").setLevel(logging.WARNING)
    else:
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
        logging.getLogger("paramiko").setLevel(logging.INFO)
    
    # Return logger for convenience
    return logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# end-to-end pipeline for downloading all media
# ------------------------------------------------------------------------------

def full_pipeline(debug: bool):
    """
    full pipeline for downloading contents
    :param debug: param indicating whether to run in debug mode
    """

    logger = setup_logging(debug=debug)

    core.rss_ingest()
    core.collect_media()
    core.parse_media()
    core.collect_metadata()
    core.filter_media()
    core.initiate_media_download()
    core.check_downloads()
    core.transfer_media()
    core.cleanup_media()

# -------------------------------------------------------------------------------
# main functions
# -------------------------------------------------------------------------------

def main():
    """
    Main function for command-line interface
    :return:
    """
    parser = argparse.ArgumentParser(
        description="Command-line interface for running functions"
    )

    # Add global debug flag
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug mode"
    )

    args = parser.parse_args()

    # Use the mapping to call full_pipeline
    full_pipeline(debug=args.debug)

# ------------------------------------------------------------------------------
# main clause
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    main()

# ------------------------------------------------------------------------------
# end of main.py
# ------------------------------------------------------------------------------