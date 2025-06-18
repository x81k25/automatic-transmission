# standard library imports
import logging

# local/custom imports
from src.data_models import *
import src.utils as utils

# ------------------------------------------------------------------------------
# supporting functions
# ------------------------------------------------------------------------------

def confirm_downloading_status(
    media: MediaDataFrame,
    current_media_items: dict | None
) -> MediaDataFrame:
    """
    determine if items tagged as pipeline_status = 'downloading' are in fact
        downloading

    :param media: MediaDataFrame are all 'downloading' items
    :param current_media_items: dict of media current items in transmission
    :return:
    """
    media_not_downloading = media.df.clone()

    # if database items in downloading state, but no items with transmission
    #   label error condition, which will trigger re-ingest
    if current_media_items is None:
        media_not_downloading = media_not_downloading.with_columns(
            error_condition=pl.lit("not found within transmission")
        )
    # if only some items missing from transmission, label only the items
    #   missing
    else:
        current_hashes = list(current_media_items.keys())

        media_not_downloading = media_not_downloading.filter(
            ~pl.col('hash').is_in(current_hashes)
        ).with_columns(
            error_condition = pl.lit("not found within transmission")
        )

    return MediaDataFrame(media_not_downloading)


def extract_and_verify_filename(
    media: MediaDataFrame,
    downloaded_media_items: dict
) -> MediaDataFrame:
    """
    extracts, verifies, and inserts original_path to MediaDataFrame

    :param media: MediaDataFrame without original_path
    :param downloaded_media_items: metadata for items in transmission
    :return: dict with updated file_name or error information
    """
    media_with_paths = media.df.clone()

    # get file paths
    original_file_paths = pl.DataFrame([
        {'hash': k, 'original_path': v['name']}
        for k, v in downloaded_media_items.items()
    ]).with_columns(
        original_path = pl.col("original_path").fill_null("None")
    ).with_columns(
        error_condition = pl.when(
            pl.col("original_path") == "None")
                .then(pl.lit("media_item.name returned None object"))
            .when(pl.col("original_path") == "")
                .then(pl.lit("media_item.name returned empty string"))
            .when(pl.col("original_path") == pl.col("hash"))
                .then(pl.lit("media_item.name contains item hash not filename"))
            .otherwise(pl.lit(None))
    ).with_columns(
        original_path = pl.when(
            pl.col('original_path') == "None")
                .then(pl.lit(None))
            .otherwise(pl.col('original_path'))
    )

    media_with_paths = media_with_paths.update(
        original_file_paths,
        on='hash'
    )

    return MediaDataFrame(media_with_paths)


def update_status(media: MediaDataFrame) -> MediaDataFrame:
    """
    updates status flags based off of conditions

    :param media: MediaDataFrame with old status flags
    :return: updated MediaDataFrame with correct status flags
    """
    media_with_updated_status = media.df.clone()

    media_with_updated_status = media_with_updated_status.with_columns(
        pipeline_status = pl.when(pl.col('error_condition') == "not found within transmission")
            .then(pl.lit(PipelineStatus.INGESTED))
        .when(
            (~pl.col('error_status')) &
            (pl.col('rejection_status') != RejectionStatus.REJECTED)
        )
            .then(pl.lit(PipelineStatus.DOWNLOADED))
        .otherwise(pl.col('pipeline_status'))
    )

    # if re-ingesting remove flags
    media_with_updated_status = media_with_updated_status.with_columns(
        rejection_status = pl.when(pl.col('rejection_status') == RejectionStatus.OVERRIDE)
            .then(pl.lit(RejectionStatus.OVERRIDE))
            .otherwise(
                pl.when(pl.col('pipeline_status') == PipelineStatus.INGESTED)
                    .then(pl.lit(RejectionStatus.UNFILTERED))
                .otherwise(pl.col('rejection_status'))
            ),
        rejection_reason = pl.when(pl.col('pipeline_status') == PipelineStatus.INGESTED)
            .then(pl.lit(None))
            .otherwise(pl.col('rejection_reason')),
        error_status = pl.when(pl.col('pipeline_status') == PipelineStatus.INGESTED)
            .then(pl.lit(False))
            .otherwise(pl.col('error_status')),
        error_condition = pl.when(pl.col('pipeline_status') == PipelineStatus.INGESTED)
            .then(pl.lit(None))
            .otherwise(pl.col('error_condition'))
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
        if row['pipeline_status'] == PipelineStatus.INGESTED:
            logging.error(f"re-ingesting - {row['hash']}")
        elif row['error_status']:
            logging.error(f"{row['hash']} - {row['error_condition']}")
        elif row['rejection_status'] == RejectionStatus.REJECTED:
            logging.info(f"{row['rejection_status']} - {row['hash']} - {row['rejection_reason']}")
        else:
            logging.info(f"{row['pipeline_status']} - {row['hash']}")


# ------------------------------------------------------------------------------
# main check download function for all media items
# ------------------------------------------------------------------------------

def check_downloads():
    """
    check downloads for all downloading media elements, and extracts file_name
        if download is complete
    """
    # read in existing data based on ingest_type
    media = utils.get_media_from_db(pipeline_status='downloading')

    # get current media item info
    current_media_items = utils.return_current_media_items()

    # if no media items and no transmission items, return
    if media is None and current_media_items is None:
        return

    # process items for re-ingestion
    media_not_downloading = confirm_downloading_status(
        media,
        current_media_items
    )

    # re-ingest items not downloading if needed
    if media_not_downloading.height > 0:
        media_not_downloading = update_status(media_not_downloading)
        utils.media_db_update(media=media_not_downloading.to_schema())
        log_status(media_not_downloading)

        # remove reingested items from processing
        media.update(
            media.df.join(media_not_downloading.df.select('hash'), on='hash', how='anti')
        )

    # if no current_media_items, return
    if current_media_items is None:
        return

    # filter by items with download complete
    downloaded_media_items = {k: v for k, v in current_media_items.items() if v['progress'] == 100.0}

    media = media.filter(pl.col('hash').is_in(downloaded_media_items))

    # if no items downloaded, return
    if media.height == 0:
        return

    # extract file names from completed downloads
    media = extract_and_verify_filename(media, downloaded_media_items)

    # update status, commit to db, and log
    media = update_status(media)
    utils.media_db_update(media=media.to_schema())
    log_status(media)


# ------------------------------------------------------------------------------
# main guard
# ------------------------------------------------------------------------------

def main():
    utils.setup_logging()
    check_downloads()

if __name__ == "__main__":
    main()


# ------------------------------------------------------------------------------
# end of _08_download_check.py
# ------------------------------------------------------------------------------