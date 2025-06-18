# standard library imports
import logging
import os

# third party imports
from dotenv import load_dotenv

# local/custom imports
from src.data_models import *
import src.utils as utils

# ------------------------------------------------------------------------------
# utility functions
# ------------------------------------------------------------------------------

def initiate_media_item(media_item: dict) -> dict:
    """
    attempts to initiate media items, and if collects error information if
        initiation fails
    :param media_item: contains data of individual
    :return media_item: updated dict containing status and/or error data

    :debug:
    """
    # attempt to initiate each item
    try:
        utils.add_media_item(media_item['hash'])
    except Exception as e:
        media_item['error_condition'] = f"initiate_item error: {e}"

    return media_item


def update_status(media: MediaDataFrame) -> MediaDataFrame:
    """
    updates status flags based off of conditions

    :param media: MediaDataFrame with old status flags
    :return: updated MediaDataFrame with correct status flags
    """
    media_with_updated_status = media.df.clone()

    media_with_updated_status = media_with_updated_status.with_columns(
        pipeline_status = pl.when(
            (pl.col('rejection_status').is_in([RejectionStatus.ACCEPTED.value, RejectionStatus.OVERRIDE.value])) &
            (pl.col('error_status') == False)
        ).then(pl.lit(PipelineStatus.DOWNLOADING))
        .when(pl.col('rejection_status') == RejectionStatus.REJECTED)
            .then(pl.lit(PipelineStatus.REJECTED))
        .otherwise(pl.col('pipeline_status'))
    )

    return MediaDataFrame(media_with_updated_status)


def log_status(media: MediaDataFrame) -> None:
    """
    logs current stats of all media items

    :param media: MediaDataFrame contain process values to be printed
    :return: None
    """
    # log entries based on rejection status
    for idx, row in enumerate(media.df.iter_rows(named=True)):
        if row['error_status']:
            logging.error(f"{row['hash']} - {row['error_condition']}")
        elif row['rejection_status'] == RejectionStatus.REJECTED:
            logging.info(f"{row['rejection_status']} - {row['hash']} - {row['rejection_reason']}")
        else:
            logging.info(f"{row['pipeline_status']} - {row['hash']}")

# ------------------------------------------------------------------------------
# full initiation pipeline
# ------------------------------------------------------------------------------

def initiate_media_download():
    """
    attempts to initiate all media elements currently in the queued state

    :debug: batch=0
    """
    # pipeline env vars
    batch_size = os.getenv('BATCH_SIZE')

    # read in existing data based
    media = utils.get_media_from_db(pipeline_status=PipelineStatus.MEDIA_ACCEPTED)

    # if no items to initiate, then return
    if media is None:
        return

     # batch up operations to avoid API rate limiting
    number_of_batches = (media.df.height + 49) // 50

    for batch in range(number_of_batches):
        logging.debug(f"starting initiation batch {batch+1}/{number_of_batches}")

        # set batch indices
        batch_start_index = batch * 50
        batch_end_index = min((batch + 1) * 50, media.df.height)

        # create media batch as proper MediaDataFrame to perform data validation
        media_batch = MediaDataFrame(media.df[batch_start_index:batch_end_index])

        try:
            # initiate all queued downloads
            updated_rows = []
            for idx, row in enumerate(media_batch.df.iter_rows(named=True)):
                updated_row = initiate_media_item(row)
                updated_rows.append(updated_row)

            media_batch.update(pl.DataFrame(updated_rows))

            logging.debug(f"completed initiation batch {batch+1}/{number_of_batches}")

        except Exception as e:
            # log errors to individual elements
            media_batch.update(
                media_batch.df.with_columns(
                    error_status = pl.lit(True),
                    error_condition = pl.lit(f"batch error - {e}")
                )
            )

            logging.error(f"initiation batch {batch+1}/{number_of_batches} failed - {e}")

        media_batch = update_status(media_batch)
        utils.media_db_update(media=media_batch.to_schema())
        log_status(media_batch)


# ------------------------------------------------------------------------------
# main guard
# ------------------------------------------------------------------------------

def main():
    utils.setup_logging()
    load_dotenv(override=True)
    initiate_media_download()

if __name__ == "__main__":
    main()


# ------------------------------------------------------------------------------
# end of _07_initiate.py
# ------------------------------------------------------------------------------