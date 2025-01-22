import src.core as core
import argparse

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

def full_pipeline(media_type):
    core.rss_ingest(media_type=media_type)
    core.collect_media(media_type=media_type)
    core.parse_media(media_type=media_type)
    core.collect_metadata(media_type=media_type)
    core.filter_media(media_type=media_type)
    core.initiate_media_download(media_type=media_type)
    core.check_downloads(media_type=media_type)
    core.transfer_media(media_type=media_type)
    core.cleanup_media(media_type=media_type)

#-------------------------------------------------------------------------------
# main functions
#-------------------------------------------------------------------------------

def main():
    # Create an argument parser
    parser = argparse.ArgumentParser(
       description="Command-line interface for running functions"
    )

    # Create subparsers for different functions
    subparsers = parser.add_subparsers(
        dest="command",
        help="Available functions"
    )

    # Subparser for movie pipeline
    parser_one = subparsers.add_parser(
        "movie_pipeline",
        help="Execute movie pipeline"
    )

    # Subparser for tv show pipeline
    parser_two = subparsers.add_parser(
        "tv_show_pipeline",
        help="Execute tv show pipeline"
    )

    # Subparser for tv season pipeline
    parser_three = subparsers.add_parser(
        "tv_season_pipeline",
        help="Execute tv season pipeline"
    )

    # Parse the arguments from the command line
    args = parser.parse_args()

    # Logic to call the unified function with appropriate media type
    if args.command == "movie_pipeline":
        full_pipeline(media_type="movie")
    elif args.command == "tv_show_pipeline":
        full_pipeline(media_type="tv_show")
    elif args.command == "tv_season_pipeline":
        full_pipeline(media_type="tv_season")
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
