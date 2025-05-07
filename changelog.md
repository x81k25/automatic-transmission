# Changelog

## [1.2.6]
- added TMDB, and specifically TMDB search to the metadata collection pipeline
  - the metadata collection now has 3 phases
    - the first phase searches TMDB base off of the parsed title string
    - the top match is returned, and re-ingested to TMDB for additional metadata
    - the OMDb API is then queried for additional metadata
- the TMDB search functionality catches many of the string edge cases, so we have simplified title parsing
  - the post-processing string replacement step has been removed
  - the extract_title functions has been simplified to only return the portion of the title before season/episode/year identifiers and remove some special characters 
- added _09_cleanup function to remove hung items
- gradually transitioning all logging statements to use the item hash rather than the original_tile and "-" instead of ":" 
- altered extract_title function to return None if no meaningful title can be extracted

## [1.2.5]
- fixed issues with setting all not null status fields on ingest
- fixed issue where parser would overwrite season number for tv_shows
- altered download_check logic so that items missing from the daemon will be re-ingested
- added UTC timestamp to MediaDataFrame to align with database definition
- added stricter type enforcement and validation for the MediaDataFrame object, especially for datetime rows 

## [1.2.4]
- moved tables tv_shows, tv_season, and movies into consolidated media table that accepts all values
- transitioned from baremetal postgres database to k8s databases partitioned by deployment environment

## [1.2.3]
- transitioned from pandas to polars throughout the entire application
- re-oriented all data operations around new MediaDataFrame class defined in src.data_models
- added fields for error_handling
  - error_status is boolean value, when true will halt item flow in pipeline
  - error_condition is a varchar that contains information about the error
- added cleanup delay environment variable
  - media items will now wait until the clean up delay has been reached after they are transferred, to be deleted 
- added error handling utils script with various functions to perform ad-hoc error correction in the pipeline

## [1.2.2]
- change all file operations to run local via os, pathlib and shutil; replacing the older ssh options; depracating ssh options

## [1.2.1]
- change media collection now; if API response is successful but item not in database, item is rejected 

## [1.2.0]
- alter db update statement to be more robust to deal with torrents being manually demoted to previous status
- created utils.parse_element and moved all parsing functions to this module; in order to standardize parsing across different src scripts that parsed the same element

## [1.1.7]
- removed internal logger function and replaced withe standard python logging package
- changed from ShowRSS to episodefeed for tv shows

## [1.1.6]
- added missing status update in cleanup
- added supports for tv_seasons to the rest of the pipeline
  - added move_tv_season to sshf.py
- removed delete component within sshf.py, as deletion will now be handled by cleanup
- made media initiation type agnostic utilizing new torrent_source field

## [1.1.5]
- fixed more issues with parsing
- change the flow of the pipeline operations
  - download_check will not be conducted after initiate
    - download_check will check download status and mark_download complete
  - clean_up will not be the final stage in the pipeline
    - clean_up will remove the torrent and from transmission
    - clean_up will remove the original file from the completed downloads folder
  - altered sqlf.py to generate engine as needed per function
  - altered rpcf.py to generate transmission client as needed per function

## [1.1.4]
- changed the input for transfer from file_name to raw_title
  - appears to work around issue where file_name was not being properly extracted
- made several small changes to the regex in collect to account for edge cases
  - file names that use periods to separate words will now be appropriately parsed
  - move or tv show names that include "Its" will now be automatically changed to "It's"

## [1.1.3]
- altered collect and filter scripts to properly populate rejection_status
- collect now includes items that have previously been rejected
- filter will now accept all items that have rejection_status = override
  - rejection status will remain "override" after filter is complete
  - print output will reflect this
- all torrent added ad hoc should now be added to automated pipeline

## [1.1.2]
- utils.log now print to console rather than directly to log file; logs will be captured from console and migrated to log file
- added src.core._02_collect
  - collect will search for torrents download ad hoc
  - ad hoc torrents will be scanned to test for inclusion in tv_show or movie database
  - once collected they will become part of the standard download flow
  - currently only individual episodes and movies will be collected
  - later version will include functionality to collect entire seasons
- made database changes to reflect the multiple sources of torrents
  - all torrent links, regardless of type, will be stored in the torrent_source field

## [1.1.1]
- added special condition to convert tv_show_title "60 Minutes (US)" to "60 Minutes"
- added error handling for failing to connect to SQL database

## [1.1.0]
- reconfigured to use database rather than storing data internally in local objects
- reconfigured test/dev environment to use transmission instance hosted via WSL and not on production server
- change the name of the log function from "logger" to "log"
- added some unit tests and an overall integration test

## [1.0.0]
- first completed working version
- supports movie and tv show downloads based off parameters set by user
- can be deployed on windows or linux