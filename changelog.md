# Changelog

## [1.1.2]
- utils.log now print to console rather than directly to log file; logs will be captured from console and migrated to log file
- added src.core._02_collect
  - collect will search for torrents download ad hoc
  - ad hoc torrents will be scanned to test for inclusion in tv_show or movie database
  - once collected they will become part of the standard download flow
  - currently only individual episodes and movies will be collected
  - later version will include functionality to collect entire seasons

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