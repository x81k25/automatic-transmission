-- this script is less rigid than than back-up-create.sql
-- it will require customization and multiple additions throughout
-- please review and execute carefully

-- the target schema will need to be created with instantiate-schema.sql before this script may be executed
-- perform any additions or alters necessary on the bak tables before migrating to target schema

-- cast all text elements as apporpriate type elemenst where required
alter table bak.movies
alter column status type <target_schema>.media_status
USING status::<target_schema>.media_status;

alter table bak.movies
alter column rejection_status type <target_schema>.rejection_status
USING rejection_status::<target_schema>.rejection_status;

alter table bak.tv_shows
alter column status type <target_schema>.media_status
USING status::<target_schema>.media_status;

alter table bak.tv_shows
alter column rejection_status type <target_schema>.rejection_status
USING rejection_status::<target_schema>.rejection_status;

-- move the table data
INSERT INTO <target_schema>.movies (
   hash,
   raw_title,
   movie_title,
   release_year,
   status,
   torrent_source,
   rejection_status,
   rejection_reason,
   published_timestamp,
   summary,
   genre,
   language,
   metascore,
   rt_score,
   imdb_rating,
   imdb_votes,
   imdb_id,
   resolution,
   video_codec,
   upload_type,
   audio_codec,
   file_name,
   uploader,
   created_at,
   updated_at
)
SELECT
   hash,
   raw_title,
   movie_title,
   release_year,
   status::<target_schema>.media_status,
   torrent_source,
   COALESCE(rejection_status::<target_schema>.rejection_status, 'unfiltered'::<target_schema>.rejection_status),
   rejection_reason,
   published_timestamp,
   summary,
   genre::_text,
   language::_text,
   metascore,
   rt_score,
   imdb_rating,
   imdb_votes,
   imdb_id,
   resolution,
   video_codec,
   upload_type,
   audio_codec,
   file_name,
   uploader,
   created_at,
   updated_at
FROM bak.movies;

INSERT INTO <target_schema>.tv_shows (
   hash,
   raw_title,
   tv_show_name,
   season,
   episode,
   status,
   torrent_source,
   published_timestamp,
   rejection_status,
   rejection_reason,
   summary,
   release_year,
   genre,
   language,
   metascore,
   imdb_rating,
   imdb_votes,
   imdb_id,
   resolution,
   video_codec,
   upload_type,
   audio_codec,
   file_name,
   created_at,
   updated_at
)
SELECT
   hash,
   raw_title,
   tv_show_name,
   season,
   episode,
   status::<target_schema>.media_status,
   torrent_source,
   published_timestamp,
   COALESCE(rejection_status::<target_schema>.rejection_status, 'unfiltered'::<target_schema>.rejection_status),
   rejection_reason,
   summary,
   release_year,
   genre::_text,
   language::_text,
   metascore,
   imdb_rating,
   imdb_votes,
   imdb_id,
   resolution,
   video_codec,
   upload_type,
   audio_codec,
   file_name,
   created_at,
   updated_at
FROM bak.tv_shows;