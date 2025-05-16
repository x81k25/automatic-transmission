WITH grouped_media AS (
    SELECT
        imdb_id,
        ARRAY_AGG(rejection_status) AS rejection_statuses,
        -- Take first non-null value for each field
        MAX(tmdb_id) AS tmdb_id,
        MAX(media_type) AS media_type,
        MAX(media_title) AS media_title,
        MAX(season) AS season,
        MAX(episode) AS episode,
        MAX(release_year) AS release_year,
        MAX(genre) AS genre,
        MAX(language) AS language,
        MAX(rt_score) AS rt_score,
        MAX(metascore) AS metascore,
        MAX(imdb_rating) AS imdb_rating,
        MAX(imdb_votes) AS imdb_votes,
        CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS created_at,
        CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS updated_at
    FROM {{ ref('media') }}
    WHERE imdb_id IS NOT NULL
    GROUP BY imdb_id
),
processed_media AS (
    SELECT
        *,
        -- Check if any rejection_status is 'accepted' or 'override'
        CASE
            WHEN 'accepted' = ANY(rejection_statuses) OR 'override' = ANY(rejection_statuses) THEN 1
            WHEN 'unfiltered' = ALL(rejection_statuses) THEN NULL
            ELSE 0
        END AS label
    FROM grouped_media
)

SELECT
    imdb_id,
    tmdb_id,
    label,
    media_type,
    COALESCE(media_title, '') AS media_title, -- Ensure NOT NULL constraint
    season::SMALLINT,
    episode::SMALLINT,
    release_year::SMALLINT,
    genre,
    language,
    rt_score::SMALLINT,
    metascore::SMALLINT,
    imdb_rating,
    imdb_votes,
    created_at,
    updated_at
FROM processed_media
WHERE label IS NOT NULL