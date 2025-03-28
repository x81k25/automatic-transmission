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
# end-to-end pipeline for downloading contents
# accepts media_type = "movie" or media_type = "tv_show"
#
# to execute the tv show pipeline
# cd <automatic-transmission-dir> & python main.py movie_pipeline
#
# to execute the tv show pipeline
# cd <automatic-transmission-dir> & python main.py tv_show_pipeline
#
#statements for testing
#media_type = "movie"
#media_type = "tv_show"
#media_type = "tv_season"
#
# ------------------------------------------------------------------------------

def full_pipeline(
    media_type: str,
    debug: bool
):
    """
    full pipeline for downloading contents
    :param media_type: either "movie", "tv_show", or "tv_season
    :param debug: param indicating whether to run in debug mode
    """
    if media_type not in ["movie", "tv_show", "tv_season"]:
        raise ValueError(f"Invalid media_type: {media_type}")

    logger = setup_logging(debug=debug)

    core.rss_ingest(media_type=media_type)
    core.collect_media(media_type=media_type)
    core.parse_media(media_type=media_type)
    core.collect_metadata(media_type=media_type)
    core.filter_media(media_type=media_type)
    core.initiate_media_download(media_type=media_type)
    core.check_downloads(media_type=media_type)
    core.transfer_media(media_type=media_type)
    core.cleanup_media(media_type=media_type)

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

    subparsers = parser.add_subparsers(
        dest="command",
        help="Available functions",
        required=True  # Make subcommand required
    )

    # Add subparsers with consistent naming
    subparsers.add_parser(
        "movie_pipeline",
        help="Execute movie pipeline"
    )

    subparsers.add_parser(
        "tv_show_pipeline",
        help="Execute tv show pipeline"
    )

    subparsers.add_parser(
        "tv_season_pipeline",
        help="Execute tv season pipeline"
    )

    args = parser.parse_args()

    # Create a mapping of commands to media types
    command_to_media = {
        "movie_pipeline": "movie",
        "tv_show_pipeline": "tv_show",
        "tv_season_pipeline": "tv_season"
    }

    # Use the mapping to call full_pipeline
    if args.command in command_to_media:
        full_pipeline(
            media_type=command_to_media[args.command],
            debug=args.debug
        )
    else:
        parser.print_help()

# ------------------------------------------------------------------------------
# main clause
# ------------------------------------------------------------------------------

if __name__ == "__main__":
    main()

# ------------------------------------------------------------------------------
# end of main.py
# ------------------------------------------------------------------------------