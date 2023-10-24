############################################################################
############################################################################
###                                                                      ###
###                  TRANSFORM TRANSIT SCHEDULE TO GTFS                  ###
###                                                                      ###
############################################################################
############################################################################

# DATE: 2023-06-23

# Load packages -----------------------------------------------------------

#remotes::install_github("ITSleeds/UK2GTFS")
library(UK2GTFS)
library(tidyverse)
library(sf)
library(mapview)
library(gtfstools)


# Add missing stops -------------------------------------------------------

# Define stops missing in tiplocs
extra_tiplocs <- 
  tribble(
    ~stop_id, ~stop_code, ~stop_name, ~stop_lon, ~stop_lat,
    # Same location to usual entrance of Westbourne Park St.
    "WBRNPKS", "WBRNPKCS", "Westbourne Park Carriage Sidings",  -0.201175, 51.520894,
    # The below does not really affect since it is a long-distance (LND-GLA)
    #"WMBYEFR", NA, "Wembley Reception Sidings 1-7", NA, NA,
    # Rum Ferry Terminal
    "RHUM", NA_character_, "Rum Ferry Terminal", -6.265116, 57.010849,
    # Port Mor in Muck
    "MUCK", NA_character_, 'Muck Ferry Terminal', -6.226735, 56.833650, 
    # Eigg
    "EIGG", NA_character_, 'Eigg Pier', -6.127477, 56.879621,
    # Canna Island
    "CANNA", NA_character_, 'Canna Ferry Terminal',  -6.490809, 57.055999,
  ) %>% 
  st_as_sf(coords = c('stop_lon', 'stop_lat'), crs = 4326)

# Add stops to Tiplocs
tiplocs_extra <- bind_rows(tiplocs, extra_tiplocs)


# ATOC files to GTFS - (rail) ---------------------------------------------

# Cores
n_cores <- parallel::detectCores() - 1

# Transform atoc to gtfs
path_in <- "data/timetable/atoc/20230304/ttis662.zip"
ttis662 <- atoc2gtfs(
  path_in = path_in, 
  shapes = TRUE, 
  locations = tiplocs_extra,
  ncores = n_cores
)

# Warning: Adding 22 missing tiplocs, these may have unreliable location data

# Visualise stops that are not in tiploc file
ttis662$stops %>% 
  filter(!stop_id %in% tiplocs_extra$stop_id) %>% 
  st_as_sf(coords = c('stop_lon', 'stop_lat'), crs = 4326) %>% 
  mapview()

## Inspect calendar
summary(parse_date(ttis662$calendar$start_date, format = "%Y%m%d"))
# Three quarters of the services start on or after the 2023-03-11 (Q1)
summary(parse_date(ttis662$calendar$end_date, format = "%Y%m%d"))
# Three quarters of the services end on or after the 2023-03-26

# # Stops
# ttis543$stops %>%
#   st_as_sf(coords = c("stop_lon", "stop_lat"), crs = 4326) %>%
#   mapview()

# Check internal validity
UK2GTFS::gtfs_validate_internal(ttis662)
# Warning messages:
#   1: In UK2GTFS::gtfs_validate_internal(ttis662) : NA values in stops


## Force valid. This function does not fix problems it just removes them
ttis_gtfs <- UK2GTFS::gtfs_force_valid(ttis662)
## Compare original and valid
# Find difference
map2(ttis662, ttis_gtfs, identical)
# No differences
# # Stop times not included in GTFS version
# anti_join(ttis662$stop_times, ttis_gtfs$stop_times)

# # Check context of missing stops 
# stops_missing <- ttis662$stop_times %>% 
#   filter(trip_id == 92114) %>% 
#   pull(stop_id)
# # Map stations around missing stop
# ttis662$stops %>% 
#   filter(stop_id %in% stops_missing) %>% 
#   st_as_sf(coords = c('stop_lon', 'stop_lat'), crs = 4326) %>% 
#   mapview()


## Write as GTFS
UK2GTFS::gtfs_write(
  ttis_gtfs, 
  folder = "data/timetable/atoc/", 
  name = "ttis662.gtfs"
)

# Clean env.
rm(list = ls())
gc(reset = TRUE)



# Transform Traveline data ------------------------------------------------

# Cores
n_cores <- parallel::detectCores() - 1
# Date of feeds
date_feed <- "20230307"

# Output GTFS dir
out_dir <- paste0("data/timetable/traveline/", date_feed, '/gtfs/')
dir.create(out_dir)

# List of TXC feeds by region
list_feeds <- list.files(
  paste0("data/timetable/traveline/", date_feed), 
  recursive = TRUE,
  pattern = 'zip$', 
  full.names = TRUE
)

# GTFS validator
validator_path <- download_validator(out_dir)

# Run the for loop
for (feed in list_feeds) {
  
  # Folder for feed
  feed_path <- paste0(out_dir, sub("\\.zip", '', basename(feed)), '/')
  dir.create(feed_path)
  # Feed name
  feed_name <- paste0(sub("\\.zip", '', basename(feed)), ".gtfs")
  # Prelim name
  name_prel <- paste0('prelim-', feed_name)
  
  # Transform TXC to GTFS
  tline_gtfs <- UK2GTFS::transxchange2gtfs(
    path_in = feed,
    scotland = 'auto',
    ncores = n_cores, 
    force_merge = TRUE,
    silent = FALSE
  )
  
  # Write preliminary feed
  UK2GTFS::gtfs_write(
    gtfs = tline_gtfs,
    folder = feed_path, 
    name = name_prel
  )
  
  # Validate preliminary output
  gtfstools::validate_gtfs(
    gtfs = paste0(feed_path, name_prel, '.zip'), 
    output_path = feed_path,
    validator_path = validator_path, 
    html_preview = FALSE,
    overwrite = TRUE
  )
  
  # Make valid
  tline_gtfs <- UK2GTFS::gtfs_force_valid(tline_gtfs)
  
  # Write valid feed
  UK2GTFS::gtfs_write(
    gtfs = tline_gtfs,
    folder = feed_path, 
    name = feed_name
  )
}


# Merge traveline GTFS ----------------------------------------------------

# Traveline files
traveline_gtfs_dirs <- 
  list.files(
    paste0("data/timetable/traveline/", date_feed),
    pattern = "gtfs\\.zip",
    recursive = TRUE,
    full.names = TRUE
  )
traveline_gtfs_dirs <- traveline_gtfs_dirs[!grepl("prelim", traveline_gtfs_dirs)]

# Read traveline files
traveline_list <- lapply(traveline_gtfs_dirs, UK2GTFS::gtfs_read)


# # Merge
# traveline_all <- UK2GTFS::gtfs_merge(traveline_list, force = TRUE)
# # Warning message:
# #   In UK2GTFS::gtfs_merge(traveline_list, force = TRUE) :
# #   Duplicated Agency IDs 1 3 DC 2 4 SPCT STOT CTG RRS A2BR ARBB ARHE GLAR MKEN MN OBUS RRCT SK SV SWC NWPT DGC ATL WCT SCR 7778462 7778466 7778487 7778489 7778497 7778499 7778505 7778536 ACT HDT HG RS ST STL STN will be merged
# 
# # Create dir
# all_dir <- paste0("data/timetable/traveline/", date_feed, '/gtfs/all/')
# dir.create(all_dir)
# 
# # Write merged feed
# UK2GTFS::gtfs_write(
#   gtfs = traveline_all,
#   folder = all_dir, 
#   name = 'all.gtfs'
# )

# Re-write Bus Open Data feed ---------------------------------------------

# Read feed
obd_feed <- 'data/timetable/busopendata/bod_2023-03-07_12.00.01_gtfs.zip'
busopendata <- gtfstools::read_gtfs(obd_feed)

# Transform frequencies to stop times
converted_busopendata <- gtfstools::frequencies_to_stop_times(busopendata)

# Out directory
feed_date <- str_extract(obd_feed, "\\d{4}-\\d{2}-\\d{2}")
feed_date <- gsub("-", '', feed_date)
out_bod_dir <- paste0('data/timetable/busopendata/', feed_date, '/')
dir.create(out_bod_dir)
# Out feed
out_bod_feed <- paste0(out_bod_dir, 'bods_', feed_date, '.gtfs.zip')

# Write feed
gtfstools::write_gtfs(
  gtfs = converted_busopendata, 
  path = out_bod_feed
)

# GTFS validator
obd_validator_path <- download_validator(out_bod_dir)

# Validate feed
validate_gtfs(
  gtfs = out_bod_feed, 
  output_path = out_bod_dir,
  validator_path = obd_validator_path, 
  html_preview = FALSE,
  overwrite = TRUE
)


# Clean env.
rm(list = ls())
