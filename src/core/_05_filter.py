# standard library imports
import logging
import os
import requests

# third-party imports
from dotenv import load_dotenv
import polars as pl
import yaml

# local/custom imports
import src.utils as utils
from src.data_models import MediaDataFrame

# ------------------------------------------------------------------------------
# read in static parameters
# ------------------------------------------------------------------------------

# logger config
logger = logging.getLogger(__name__)

# if not inherited set parameters here
if __name__ == "__main__" or not logger.handlers:
    # Set up standalone logging for testing
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(module)s - %(funcName)s - %(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)
    logging.getLogger("paramiko").setLevel(logging.INFO)
    # Prevent propagation to avoid duplicate logs
    logger.propagate = False

# get reel-driver env vars
load_dotenv(override=True)

# pipeline env vars
batch_size = int(os.getenv('BATCH_SIZE'))
acceptance_threshold = float(os.getenv('REEL_DRIVER_THRESHOLD'))

# reel_driver_env vars
api_host = os.getenv('REEL_DRIVER_HOST')
api_port = os.getenv('REEL_DRIVER_POST')
api_prefix = os.getenv('REEL_DRIVER_PREFIX')

# get filter params
with open('./config/filter-parameters.yaml', 'r') as file:
    filters = yaml.safe_load(file)

# ------------------------------------------------------------------------------
# initiation helper functions
# ------------------------------------------------------------------------------

def filter_by_file_metadata(media_item: dict) -> dict:
    """
    filters media based off its file parameters, e.g. resolution, coded, etc.

    :param media_item:
    :return: dict containing the updated filtered data

    :debug: filter_type = 'movie'
    """
    # pass if override status was set
    if media_item['rejection_status'] == 'override':
        return media_item

    # search separate criteria for move or tv_show
    if media_item['media_type'] == 'movie':
        sieve = filters['movie']
        # iterate over each key in the filter-parameters.json file
        for key in sieve:
            # if key is defined as not nullable, then item will be rejected if not populated
            if not sieve[key]["nullable"]:
                if media_item[key] is None:
                    media_item['rejection_reason'] = f'{key} is null'
                    break
            if isinstance(media_item[key], str):
                if "allowed_values" in sieve[key] and media_item[key] not in sieve[key]["allowed_values"]:
                    media_item['rejection_reason'] = f'{key} {media_item[key]} is not in allowed_values'
                    break
            elif isinstance(media_item[key], (int, float)):
                if "min" in sieve[key] and media_item[key] < sieve[key]["min"]:
                    media_item['rejection_reason'] = f'{key} {media_item[key]} is below min'
                    break
                elif "max" in sieve[key] and media_item[key] > sieve[key]["max"]:
                    media_item['rejection_reason'] = f'{key} {media_item[key]} is above max'
                    break
            elif isinstance(media_item[key], list):
                if "allowed_values" in sieve[key] and not any([x in sieve[key]["allowed_values"] for x in media_item[key]]):
                    media_item['rejection_reason'] = f'{key} {media_item[key]} does not include {sieve[key]["allowed_values"]}'
                    break

        # set rejection status to rejected if it fails a filter
        # value will not be set to accepted yet, as it still must pass acceptance criteria vai reel-driver API
        if media_item['rejection_reason'] is not None:
            media_item['rejection_status'] = 'rejected'

    # there are currently no filters set for tv_show or tv_season
    elif media_item['media_type'] == 'tv_show':
        pass
    elif media_item['media_type'] == 'tv_season':
        pass

    return media_item


def predict_item(media_item: dict) -> dict:
    """
    passes a single media_item to the reel-driver API and appends the
        probability to the media_item

    :param media_item: single media element as a dict
    :return: dict containing the updated filtered data

    :debug: media_item=next(media.df.iter_rows(named=True))
    """
    # pass if override status was set
    if media_item['rejection_status'] == 'override':
        return media_item

    # if media_type not movie no filtering is required
    if media_item['media_type'] != 'movie':
        media_item['rejection_status'] = 'accepted'
        return media_item

    # if movie then run prediction API
    # construct API URL
    api_url = f"http://{api_host}:{api_port}/{api_prefix}/api/predict"

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

    try:
        # Call the API
        response = requests.post(api_url, json=payload)

        # handle API errors
        if response.status_code != 200:
            media_item['error_status'] = True
            error_message = f"reel-driver-api error - {response.status_code} - {response.reason}"
            media_item['error_condition'] = error_message
            logger.error(error_message)

        # Get prediction result
        probability = response.json()['probability']

        # assign accepted rejection criteria based off of result
        if probability >= acceptance_threshold:
            media_item['rejection_status'] = 'accepted'
        else:
            media_item['rejection_status'] = 'rejected'
            media_item['rejection_reason'] = f"probability {probability:.3f} below threshold {acceptance_threshold}"

        # Return the updated media_item
        return media_item

    except requests.exceptions.RequestException as e:
        # log error in logs and db
        error_message = f"reel-driver-api error - {e}"
        media_item['error_status'] = True
        media_item['error_condition'] = error_message
        logger.error(error_message)
        return media_item


def predict_items(unfiltered_media: pl.DataFrame) -> pl.DataFrame:
    """
    hits the reel-driver batch prediction, appends the probability to the
        input df, and then returns the df

    :param unfiltered_media: polars dataframe containing all items to be
        predicted
    :return: polars df with the probability appended
    """
    # all tv_show and tv_season items currently accepted
    unfiltered_media = unfiltered_media.with_columns(
        rejection_status = pl.when(pl.col('media_type') != 'movie')
            .then(pl.lit('accepted'))
            .otherwise(pl.col('rejection_status'))
    )

    # filter by only items that will need API responses
    items_to_filter = unfiltered_media.filter(
        pl.col('media_type') == 'movie',
        pl.col('rejection_status') != 'override'
    )

    # if no items to filter, return
    if items_to_filter.height == 0:
        return  unfiltered_media

    # Construct API URL
    api_url = f"http://{api_host}:{api_port}/{api_prefix}/api/predict_batch"

    payload = {
        'items':
            items_to_filter.select([
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

    try:
        # Call the API
        response = requests.post(api_url, json=payload)

        # handle API errors
        # note: if 1 items fails, it will fail the whole batch
        if response.status_code != 200:
            error_message = f"reel-driver-api error - {response.status_code} - {response.reason}"
            unfiltered_media = unfiltered_media.with_columns(
                error_status = pl.lit(True),
                error_condition = pl.lit(error_message)
            )
            logger.error(error_message)

            return unfiltered_media
        else:
            # Get prediction result
            prediction_result = pl.DataFrame(response.json()['results']).select(
                'imdb_id',
                'probability'
            )

            # deduplicate prediction results, keeping first occurrence
            prediction_result = prediction_result.unique(subset=['imdb_id'], keep='first')

            # combine prediction into unfiltered media table
            unfiltered_media = unfiltered_media.join(prediction_result, on='imdb_id', how='left')

            # filter media based off of predictions
            filtered_media = unfiltered_media.with_columns(
                rejection_status = pl.when(
                    pl.col('probability').is_null())
                        .then(pl.col('rejection_status'))
                    .when(pl.col('probability') >= acceptance_threshold)
                        .then(pl.lit('accepted'))
                    .otherwise(pl.lit('rejected')),
                rejection_reason = pl.when(
                    pl.col('probability') < acceptance_threshold)
                        .then(pl.lit("probability ") + pl.col('probability').round(3).cast(pl.Utf8) + pl.lit(" below threshold ") + pl.lit(str(acceptance_threshold)))
                    .otherwise(pl.col('rejection_reason'))
            ).drop('probability')

            return filtered_media

    except requests.exceptions.RequestException as e:
        # log error in logs and db
        error_message = f"reel-driver-api error - {e}"
        unfiltered_media = unfiltered_media.with_columns(
                error_status = pl.lit(True),
                error_condition = pl.lit(error_message)
            )
        logger.error(error_message)
        return unfiltered_media.with_columns(probability = pl.lit(None).cast(pl.Float64))


# ------------------------------------------------------------------------------
# full filtration pipeline
# ------------------------------------------------------------------------------

def filter_media():
    """
    full pipeline for filtering all media after metadata has been collected

    :debug: media.update(media.df[3])
    :debug: batch = 0
    """
    # read in existing data based on ingest_type
    media = utils.get_media_from_db(pipeline_status='metadata_collected')

    if media is None:
        return

    # filter based off of file parameters for all elements
    updated_rows = []
    for idx, row in enumerate(media.df.iter_rows(named=True)):
        updated_row = filter_by_file_metadata(media_item=row)
        updated_rows.append(updated_row)

    media = MediaDataFrame(updated_rows)

    # separate media items based off of those that need to pass through
    #   reel-driver and those that do not
    reel_media = MediaDataFrame(
        media.df
        .filter(media_type = 'movie')
        .filter(~pl.col('rejection_status').is_in(['override', 'rejected']))
    )

    un_reel_media = MediaDataFrame(
        media.df.filter(~pl.col('hash').is_in(list(reel_media.df['hash'])))
    )

    # commit items which do not require reel-driver to db and log
    if un_reel_media.df.height > 0:
        # update status accordingly
        un_reel_media.update(un_reel_media.df.with_columns(
            pipeline_status=pl.when(
                pl.col('error_status') == True)
                    .then(pl.col('pipeline_status'))
                .otherwise(
                    pl.when(pl.col('rejection_status') == 'rejected')
                        .then(pl.lit('rejected'))
                    .otherwise(pl.lit('queued'))
                )
            )
        )

        utils.media_db_update(media=un_reel_media)
        for row in un_reel_media.df.iter_rows(named=True):
            if row['rejection_status'] == 'rejected':
                logging.info(f"{row['rejection_status']} - {row['hash']} - {row['rejection_reason']}")
            elif not row['error_status']:
                logging.info(f"queued - {row['hash']}")

    # if no items passed to reel_driver, return
    if reel_media.df.height == 0:
        return

    # filter 1 item
    elif reel_media.df.height == 1:
        reel_media.update(
            pl.DataFrame(
                [predict_item(next(reel_media.df.iter_rows(named=True)))]
            )
        )

        # log accepted and rejected entries
        for row in reel_media.df.iter_rows(named=True):
            if row['rejection_status'] == 'rejected':
                logging.info(f"rejected - {row['hash']} - {row['rejection_reason']}")
            elif row['rejection_status'] == 'accepted':
                logging.info(f"queued - {row['hash']}")

        # update pipeline_status
        reel_media.update(reel_media.df.with_columns(
            pipeline_status=pl.when(
                pl.col('error_status') == True)
                    .then(pl.col('pipeline_status'))
                .otherwise(
                    pl.when(pl.col('rejection_status') == 'rejected')
                        .then(pl.lit('rejected'))
                    .otherwise(pl.lit('queued'))
                )
            )
        )

        #write single row to database
        utils.media_db_update(media=reel_media)

    # batch filter multiple items
    else:
        # break into batches if > 50 elements in order to avoid entire queue
        #  failure due to 1 item
        number_of_batches = (reel_media.df.height + (batch_size-1)) // batch_size  # Ceiling division by 50

        for batch in range(number_of_batches):
            logging.debug(f"starting reel-driver prediction batch {batch+1}/{number_of_batches}")

            # set batch indices
            batch_start_index = batch * batch_size
            batch_end_index = min((batch + 1) * batch_size, reel_media.df.height)

            # create media batch as proper MediaDataFrame to perform data validation
            reel_media_batch = MediaDataFrame(reel_media.df[batch_start_index:batch_end_index].clone())

            try:
                # attempt to hit the prediction batch API
                reel_media_batch.update(predict_items(reel_media_batch.df))

                # update pipeline_status
                reel_media_batch.update(reel_media_batch.df.with_columns(
                    pipeline_status=pl.when(
                        pl.col('error_status') == True)
                            .then(pl.col('pipeline_status'))
                        .otherwise(
                            pl.when(pl.col('rejection_status') == 'rejected')
                                .then(pl.lit('rejected'))
                            .otherwise(pl.lit('queued'))
                        )
                    )
                )

                # log accepted and rejected entries
                for row in reel_media_batch.df.iter_rows(named=True):
                    if row['rejection_status'] == 'rejected':
                        logging.info(f"{row['rejection_status']} - {row['hash']} - {row['rejection_reason']}")
                    else:
                        logging.info(f"{row['rejection_status']} - {row['hash']}")

                logging.debug(f"completed reel-driver prediction batch {batch+1}/{number_of_batches}")

            except Exception as e:
                # log errors to individual elements
                reel_media_batch.update(
                    reel_media_batch.df.with_columns(
                        error_status = pl.lit(True),
                        error_condition = pl.lit(f"batch error - {e}")
                    )
                )

                logging.error(f"metadata collection batch {batch+1}/{number_of_batches} failed - {e}")

            try:
                # attempt to write metadata back to the database; with or without errors
                utils.media_db_update(media=reel_media_batch)
            except Exception as e:
                logging.error(f"metadata collection batch {batch+1}/{number_of_batches} failed - {e}")
                logging.error(f"metadata collection batch error could not be stored in database")


# ------------------------------------------------------------------------------
# end of _05_filter.py
# ------------------------------------------------------------------------------