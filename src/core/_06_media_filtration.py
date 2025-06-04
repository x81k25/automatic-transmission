# standard library imports
import logging
import os
import requests

# third-party imports
from dotenv import load_dotenv
import yaml

# local/custom imports
import src.utils as utils
from src.data_models import *

# -----------------------------------------------------------------------------
# read in static parameters
# -----------------------------------------------------------------------------

# get reel-driver env vars
load_dotenv(override=True)

log_level = os.getenv('LOG_LEVEL', default="INFO")

if log_level == "INFO":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.WARNING)
elif log_level == "DEBUG":
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.INFO)

# pipeline env vars
batch_size = int(os.getenv('BATCH_SIZE'))
acceptance_threshold = float(os.getenv('REEL_DRIVER_THRESHOLD'))

# reel_driver_env vars
api_host = os.getenv('REEL_DRIVER_HOST')
api_port = os.getenv('REEL_DRIVER_PORT')
api_prefix = os.getenv('REEL_DRIVER_PREFIX')

# -----------------------------------------------------------------------------
# support functions that operate on one media item at a time
# -----------------------------------------------------------------------------

def get_prediction(media_item: dict) -> dict:
    """
    passes a single media_item to the reel-driver API and appends the
        probability to the media_item

    :param media_item: single media element as a dict
    :return: dict containing the original media item with probability attached
        or the error_condition if an error occurred

    :debug: media_item=next(media.df.iter_rows(named=True))
    """
    # if media_type not movie no filtering is required
    if media_item['media_type'] != 'movie':
        media_item['rejection_status'] = RejectionStatus.ACCEPTED
        return media_item

    # construct API URL
    api_url = f"http://{api_host}:{api_port}/{api_prefix}/api/predict"

    # build payload
    payload = {
        'imdb_id': media_item.get('imdb_id'),
        'release_year': media_item.get('release_year'),
        'genre': media_item.get('genre'),
        'spoken_languages': media_item.get('spoken_languages'),
        'original_language': media_item.get('original_language'),
        'origin_country': media_item.get('origin_country'),
        'production_countries': media_item.get('production_countries'),
        'production_status': media_item.get('production_status'),
        'metascore': media_item.get('metascore'),
        'rt_score': media_item.get('rt_score'),
        'imdb_rating': media_item.get('imdb_rating'),
        'imdb_votes': media_item.get('imdb_votes'),
        'tmdb_rating': media_item.get('tmdb_rating'),
        'tmdb_votes': media_item.get('tmdb_votes'),
        'budget': media_item.get('budget'),
        'revenue': media_item.get('revenue'),
        'runtime': media_item.get('runtime'),
        'tagline': media_item.get('tagline'),
        'overview': media_item.get('overview')
    }

    # Call the API
    response = requests.post(api_url, json=payload)
    response.raise_for_status()

    media_item['probability'] = response.json()['probability']

    return media_item


# -----------------------------------------------------------------------------
# support functions that operate on multiple items as a DataFrame
# -----------------------------------------------------------------------------

def process_unfiltered_items(media: MediaDataFrame) -> MediaDataFrame:
    """
    processes items that do not need media filtering

    :param media: unprocessed MediaDataFrame
    :return: updated MediaDataFrame with the status updated to correspond
        with the label
    """
    media_unfiltered = media.filter(
        pl.col('media_type').is_in([MediaType.TV_SHOW.value, MediaType.TV_SEASON.value])
    )

    # update_status accordingly
    media_unfiltered.update(
        media_unfiltered.df.with_columns(
            rejection_status = pl.when(pl.col('rejection_status') == RejectionStatus.OVERRIDE)
                .then(pl.lit(RejectionStatus.OVERRIDE))
            .otherwise(pl.lit(RejectionStatus.ACCEPTED)),
            pipeline_status = pl.lit(PipelineStatus.MEDIA_ACCEPTED)
        )
    )

    return media_unfiltered


def reject_media_without_imdb_id(media: MediaDataFrame) -> MediaDataFrame:
    """
    set rejection_reason for items with no imdb_id

    :param media: MediaDataFrame that contains elements potentially with
        and without imdb_id's
    :return: MediaDataFrame of elements with no imdb_id
    """
    media_without_imdb_id = MediaDataFrame(
        media.df.with_columns(
            rejection_reason = pl.when(pl.col('imdb_id').is_null())
                .then(pl.lit("no imdb_id for media filtration"))
            .otherwise(pl.col('rejection_reason'))
        ).with_columns(
            rejection_status = pl.when(~pl.col('rejection_reason').is_null())
                .then(pl.lit(RejectionStatus.REJECTED))
            .otherwise(pl.col('rejection_status'))
        ).with_columns(
            pipeline_status = pl.when(pl.col('rejection_status') == RejectionStatus.REJECTED)
                .then(pl.lit(PipelineStatus.REJECTED))
            .otherwise(pl.col('pipeline_status'))
        ).filter(pl.col('pipeline_status') == PipelineStatus.REJECTED)
    )

    return media_without_imdb_id


def process_prelabeled_items(
    media: MediaDataFrame,
    media_labels: pl.DataFrame
) -> MediaDataFrame:
    """
    applies that retrieved media labels to the unprocess MediaDataFrame

    :param media: unprocessed MediaDataFrame
    :param media_labels: DataFrame containing training labels and the
        corresponding imdb_id's
    :return: updated MediaDataFrame with the status updated to correspond
        with the label
    """
    # use label to set status
    media_prelabeled = MediaDataFrame(
        media.df.filter(pl.col('imdb_id').is_in(media_labels['imdb_id'].to_list()))
        .join(
            media_labels,
            on='imdb_id',
            how='left'
        )
    )

    # update_status accordingly
    media_prelabeled.update(
        media_prelabeled.df.with_columns(
            rejection_status = pl.when(pl.col('label') == 'would_watch')
                .then(pl.lit(RejectionStatus.ACCEPTED))
            .otherwise(pl.lit(RejectionStatus.REJECTED)),
            rejection_reason = pl.when(pl.col('label') == 'would_not_watch')
                .then(pl.lit('previously failed reel-driver'))
            .otherwise(pl.lit(None))
        ).with_columns(
            pipeline_status = pl.when(pl.col('rejection_status') == RejectionStatus.ACCEPTED)
                .then(pl.lit(PipelineStatus.MEDIA_ACCEPTED))
            .otherwise(pl.lit(RejectionStatus.REJECTED))
        )
    )

    return media_prelabeled


def get_predictions(media: MediaDataFrame) -> MediaDataFrame:
    """
    hits the reel-driver batch prediction, appends the probability to the
        input df, and then returns the df

    :param media: MediaDataFrame containing all items to be
        predicted
    :return: MediaDataFrame df with the probability appended
    """
    media_with_predictions = media

    # Construct API URL
    api_url = f"http://{api_host}:{api_port}/{api_prefix}/api/predict_batch"

    # create payload with distinct imdb_id's only
    payload = {
        'items':
            media_with_predictions.df.unique(subset='imdb_id')
                .select
                    ([
                     'imdb_id',
                      'release_year',
                      'genre',
                      'spoken_languages',
                      'original_language',
                      'origin_country',
                      'production_countries',
                      'production_status',
                      'metascore',
                      'rt_score',
                      'imdb_rating',
                      'imdb_votes',
                      'tmdb_rating',
                      'tmdb_votes',
                      'budget',
                      'revenue',
                      'runtime',
                      'tagline',
                      'overview'
                ]).to_dicts()
    }

    # Call the API
    response = requests.post(api_url, json=payload)
    response.raise_for_status()

    # create new dataframe with results
    media_with_predictions = MediaDataFrame(
        media_with_predictions.df.join(
            pl.DataFrame(response.json()['results']).select(
                'imdb_id',
                'probability'
            ),
            on = 'imdb_id',
            how = 'left'
        )
    )

    return media_with_predictions


# -----------------------------------------------------------------------------
# support logic for updating statuses and corresponding status descriptions
# -----------------------------------------------------------------------------

def update_status(media: MediaDataFrame) -> MediaDataFrame:
    """
    properly updates all relevant status using the filtered MediaDataFrame

    :param media: MediaDataFrame with the probability attached or the
        error_condition if the probability could not be obtained
    :return: updated MediaDataFrame with all errors properly tagged

    :debug: media = media_batch
    """
    # if by error no probability was ever assigned, assign it now
    media_with_status = media

    if 'probability' not in media_with_status.df.columns:
        media_with_status.update(
            media_with_status.df.with_columns(
                probability = pl.lit(None).cast(pl.Float64)
            )
        )

    # check for error condition, assess probability, and set all appropriate
    #     flags
    media_with_status = MediaDataFrame(
        media_with_status.df.with_columns(
            error_condition = pl.when(
                pl.col('error_condition').is_null() &
                pl.col('probability').is_null()
            ).then(pl.lit('failed to assign probability'))
            .otherwise(pl.col('error_condition'))
        ).with_columns(
            error_status = pl.when(~pl.col('error_condition').is_null())
                .then(True)
                .otherwise(pl.col('error_status'))
        ).with_columns(
            rejection_reason = pl.when(pl.col('error_status'))
                .then(None)
            .when(pl.col('rejection_status') == RejectionStatus.OVERRIDE)
                .then(None)
            .when(pl.col('probability') < acceptance_threshold)
                .then(pl.lit("probability ") + pl.col('probability').round(3).cast(pl.Utf8) + pl.lit(" below threshold ") + pl.lit(str(acceptance_threshold)))
            .otherwise(pl.col('rejection_reason'))
        ).with_columns(
            rejection_status = pl.when(pl.col('error_status'))
                .then(pl.col('rejection_status'))
            .when(pl.col('rejection_status') == RejectionStatus.OVERRIDE)
                .then(pl.lit(RejectionStatus.OVERRIDE))
            .when(~pl.col('rejection_reason').is_null())
                .then(pl.lit(RejectionStatus.REJECTED))
            .otherwise(pl.lit(RejectionStatus.ACCEPTED))
        ).with_columns(
            pipeline_status = pl.when(pl.col('rejection_status') == RejectionStatus.ACCEPTED)
                .then(pl.lit(PipelineStatus.MEDIA_ACCEPTED))
            .when(pl.col('rejection_status') == RejectionStatus.REJECTED)
                .then(pl.lit(PipelineStatus.REJECTED))
            .otherwise(pl.col('pipeline_status'))
        )
    )

    return media_with_status


# -----------------------------------------------------------------------------
# support functions for displaying log output
# -----------------------------------------------------------------------------

def log_status(media: MediaDataFrame) -> None:
    """
    logs acceptance/rejection of the media filtration process

    :param media: MediaDataFrame contain process values to be printed
    :return: None
    """
    for row in media.df.iter_rows(named=True):
        if row['error_status']:
            logging.error(f"{row['hash']} - {row['error_condition']}")
        elif row['rejection_status'] == RejectionStatus.REJECTED:
            logging.info(f"{row['rejection_status']} - {row['hash']} - {row['rejection_reason']}")
        elif 'probability' in row:
            logging.info(f"{row['pipeline_status']} - {row['hash']} - with probability: {row['probability']:.3f}")
        else:
            logging.info(f"{row['pipeline_status']} - {row['hash']}")


# -----------------------------------------------------------------------------
# full media filtration pipeline
# -----------------------------------------------------------------------------

def filter_media():
    """
    full pipeline for filtering all media after metadata has been collected

    :debug: media.update(media.df[3])
    :debug: batch = 0
    """
    # read in existing data based on ingest_type
    media = utils.get_media_from_db(pipeline_status=PipelineStatus.METADATA_COLLECTED)

    if media is None:
        return

    # process items that do not need media filtering
    media_unfiltered = process_unfiltered_items(media)

    if media_unfiltered.df.height > 0:
        utils.media_db_update(media_unfiltered)
        log_status(media_unfiltered)

        # remove from processing
        media.update(
            media.df.join(media_unfiltered.df.select('hash'), on='hash', how='anti')
        )

    # reject items with no valid imdb_id
    media_without_imdb_id = reject_media_without_imdb_id(media)

    if media_without_imdb_id.df.height > 0:
        utils.media_db_update(media_without_imdb_id)
        log_status(media_without_imdb_id)

        # remove from processing
        media.update(
            media.df.join(media_without_imdb_id.df.select('hash'), on='hash', how='anti')
        )

    # if no media with valid imdb_id, return
    if media.df.height == 0:
        return

    # get values for media which has previously been filtered
    media_labels = utils.get_training_labels(list(set(media.df['imdb_id'])))

    # if any labels have already been set
    if media_labels is not None:
        prelabeled_media = process_prelabeled_items(media, media_labels)

        # commit to db
        utils.media_db_update(prelabeled_media.to_schema())

        # log results
        log_status(prelabeled_media)

        # remove from list of items to be filtered
        media.update(
            media.df.join(prelabeled_media.df.select('hash'), on='hash', how='anti')
        )

    # if no more items need filtration, return
    if media.df.height == 0:
        return

    # filter 1 item
    elif media.df.height == 1:
        try:
            media.update(
                pl.DataFrame(
                    [get_prediction(next(media.df.iter_rows(named=True)))]
                )
            )
        except Exception as e:
            media.df[0]['error_condition'] = f"media filtration error - {media.df[0]['hash']} - {e}"

        # handle, log, and save errors
        media = update_status(media)

        # log accepted and rejected entries
        log_status(media)

        #write single row to database
        utils.media_db_update(media=media.to_schema())

    # batch filter multiple items
    else:
        # break into batches to avoid entire queue failure due to 1 item
        number_of_batches = (media.df.height + (batch_size-1)) // batch_size  # Ceiling division by 50

        for batch in range(number_of_batches):
            logging.debug(f"starting media filtration batch {batch+1}/{number_of_batches}")

            # set batch indices
            batch_start_index = batch * batch_size
            batch_end_index = min((batch + 1) * batch_size, media.df.height)

            # create media batch as proper MediaDataFrame to perform data validation
            media_batch = MediaDataFrame(media.df[batch_start_index:batch_end_index].clone())

            try:
                # attempt to hit the prediction batch API
                media_batch = get_predictions(media_batch)

                # handle, log, and save errors
                media_batch = update_status(media_batch)

                # log accepted and rejected entries
                log_status(media_batch)

                #write single row to database
                utils.media_db_update(media=media_batch.to_schema())

            except Exception as e:
                # log errors to individual elements
                logging.error(f"media filtration batch {batch+1}/{number_of_batches} failed - {e}")

                # attempt processing of items individually
                updated_rows = []

                for idx, row in enumerate(media_batch.df.iter_rows(named=True)):
                    try:
                        updated_row = get_prediction(row)
                        updated_rows.append(updated_row)
                    except Exception as e:
                        updated_row = row
                        updated_row['error_condition'] = f"media filtration error - {media.df[0]['hash']} - {e}"
                        updated_row['probability'] = None
                        updated_rows.append(updated_row)

                media_batch.update(pl.DataFrame(updated_rows))

                # handle, log, and save errors
                media_batch = update_status(media_batch)

                # log accepted and rejected entries
                log_status(media_batch)

                #write single row to database
                utils.media_db_update(media=media_batch.to_schema())


# ------------------------------------------------------------------------------
# end of _05_filter.py
# ------------------------------------------------------------------------------