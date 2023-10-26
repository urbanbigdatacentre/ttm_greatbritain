############################################################################
############################################################################
###                                                                      ###
###                     COMPUTE TTM AT LSOA/DZ LEVEL                     ###
###                                                                      ###
############################################################################
############################################################################

# WALK TTM

# Date: 2023-10-12
# R version 4.3.0 (2023-04-21 ucrt)
# Author: J Rafael Verduzco

# This code:
# Estimate the LSOA/DZ TTM by waling by chunks
# The result are written in separated parquet files

# Libraries ---------------------------------------------------------------

# Allocate RAM to Java
options(java.parameters = "-Xmx110G")
# Load R5R, V 1.0.1
library(r5r)

library(tidyverse)
library(data.table)
library(sf)
#library(duckdb)

# Read LSOA/DZ centroids
centroids <- st_read("data/centroids/gb_lsoa_centroid2011.gpkg")
# Transform CRS
centroids <- st_transform(centroids, crs = 4326)
# Rename
centroids <- rename(centroids, id = geo_code)


# Routing parameters ------------------------------------------------------

# Route details
# Reference
# https://assets.publishing.service.gov.uk/government/uploads/system/uploads/attachment_data/file/853603/notes-and-definitions.pdf#page=6
# https://www.sthelens.gov.uk/media/331745/cd-2229-wyg_how-far-do-people-walk.pdf

# Routing inputs
mode <- c("WALK")
# Max trip duration
max_trip_duration <- 150L
# Max walking distance
# As default:  no restrictions as long as max_trip_duration is respected
max_walk <- Inf
# Walk speed
walk_speed <- 4.8
# Tuesday 7 to 10 am and 9 to 12 pm
departure_datetime <- as.POSIXct("2023-03-07 07:00:00")
# Time window
time_window <- 3 * 60
# Variation in time window
percentiles <- c(25, 50, 75)

# Bus OpenData
busopendata_router <- "router/busopendata/"
# Load traveline network
openbusdata_core <- setup_r5(data_path = busopendata_router, verbose = TRUE)


# TT public transport -----------------------------------------------------

# Create folder
out_dir <- 'output/ttm/lsoa/ttm_walk/'
dir.create(out_dir, recursive = TRUE)

# Batch size in number of origins
batch_size <- 1200

# Split origin into batches
origin_batches <- split(
  x = centroids,
  f = rep(
    1:ceiling(nrow(centroids) / batch_size),
    each = batch_size,
    length.out = nrow(centroids)
  )
)

# Open the log file for writing
log_file <- file("output/ttm/lsoa/ttm_walk/ttm_log.txt", "w")

# Run loop for time of the day
start_time <- Sys.time()
# Run loop in batches
for (i in 1:length(origin_batches)) {
  # Start time for the current loop
  loop_start_time <- Sys.time()  
  
  # Progress
  cat(
    paste(round(Sys.time()), "- Batch", i, "of", length(origin_batches)), "\n",
    file = log_file, append = TRUE
  )
  
  # Calculate ttm
  ttm_walk <- travel_time_matrix(
    r5r_core = openbusdata_core,
    origins = origin_batches[[i]],
    destinations = centroids,
    mode = mode,
    departure_datetime = departure_datetime,
    # time_window = time_window,
    # percentiles = percentiles,
    max_walk_time = max_walk,
    max_trip_duration = max_trip_duration,
    walk_speed = walk_speed
  )
  
  # # Add time of the day
  # ttm_walk[,time_of_day := departure_datetime]
  
  # Write as parquet
  arrow::write_parquet(ttm_walk, sink = paste0(out_dir, 'ttm_walk_', i, '.parquet'))
  
  # Get the number of rows in ttm_pt
  num_rows <- nrow(ttm_walk)
  
  # Write number of rows and computation time to the log file
  cat(paste("Number of rows:", num_rows), "\n",
      file = log_file, append = TRUE)
  
  loop_end_time <- Sys.time()  # End time for the current loop
  cat(paste("Loop computation time:", round(as.numeric(difftime(loop_end_time, loop_start_time, units = "secs")), 2), "secs"), "\n",
      file = log_file, append = TRUE)
  
  # Clean env
  rm(ttm_walk)
  gc()
}
end_time <- Sys.time()

# Write total computation time to the log file
cat(paste("Total computation time:", round(as.numeric(difftime(end_time, start_time, units = "secs")), 2), "secs"), "\n",
    file = log_file, append = TRUE)

# Close the log file
close(log_file)

# Stop cores
r5r::stop_r5()

# Test reading TTM --------------------------------------------------------

library(duckdb)

conn_test <- dbConnect(duckdb::duckdb())

ttm_test <- dbGetQuery(conn_test, "
     SELECT * 
     FROM read_parquet(['output/ttm/lsoa/ttm_walk/*.parquet'])
     ORDER BY ALL;
")

dbDisconnect(conn_test)


