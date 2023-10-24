############################################################################
############################################################################
###                                                                      ###
###                     COMPUTE TTM AT LSOA/DZ LEVEL                     ###
###                                                                      ###
############################################################################
############################################################################

# Bike TTM

# Date: 2023-10-23
# R version 4.3.0 (2023-04-21 ucrt)
# Author: J Rafael Verduzco

# This code:
# Estimate the LSOA/DZ TTM by bike in chunks
# The result are written in separated parquet files

# Libraries ---------------------------------------------------------------

# Allocate RAM to Java
options(java.parameters = "-Xmx60G")
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
mode <- c("BICYCLE")
# Max trip duration
max_trip_duration <- 150L
# Maximum traffic stress
traffic_stress <- 2L
# Cycling speed:
# Source: Journey time statistics
# https://www.gov.uk/government/publications/journey-time-statistics-guidance/journey-time-statistics-notes-and-definitions-2019
bike_speed <- 16 # km/ph
# Tuesday 7 to 10 am and 9 to 12 pm
departure_datetime <- as.POSIXct("2023-03-07 07:00:00")

# Bus OpenData
busopendata_router <- "router/busopendata/"
# Load traveline network
openbusdata_core <- setup_r5(data_path = busopendata_router, verbose = TRUE)


# Bicycle travel time -----------------------------------------------------


# Create folder
out_dir <- 'output/ttm/lsoa/ttm_bike/'
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
log_file <- file("output/ttm/lsoa/ttm_bike/ttm_log.txt", "w")

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
  ttm_bike <- travel_time_matrix(
    r5r_core = openbusdata_core,
    origins = origin_batches[[i]],
    destinations = centroids,
    mode = mode,
    departure_datetime = departure_datetime,
    max_trip_duration = max_trip_duration,
    max_lts = traffic_stress, 
    bike_speed = bike_speed 
  )
  
  # Write as parquet
  arrow::write_parquet(ttm_bike, sink = paste0(out_dir, 'ttm_bike_', i, '.parquet'))
  
  # Get the number of rows in ttm_pt
  num_rows <- nrow(ttm_bike)
  
  # Write number of rows and computation time to the log file
  cat(paste("Number of rows:", num_rows), "\n",
      file = log_file, append = TRUE)
  
  loop_end_time <- Sys.time()  # End time for the current loop
  cat(paste("Loop computation time:", round(as.numeric(difftime(loop_end_time, loop_start_time, units = "secs")), 2), "secs"), "\n",
      file = log_file, append = TRUE)
  
  # Clean env
  rm(ttm_bike)
  gc()
}
end_time <- Sys.time()

# Write total computation time to the log file
cat(paste("Total computation time:", round(as.numeric(difftime(end_time, start_time, units = "secs")), 2), "secs"), "\n",
    file = log_file, append = TRUE)

# Close the log file
close(log_file)


# Test reading TTM --------------------------------------------------------

library(duckdb)

conn_test <- dbConnect(duckdb::duckdb())

ttm_test <- dbGetQuery(
  conn_test, 
  "FROM read_parquet(['output/ttm/lsoa/ttm_bike/*.parquet'])"
)

dbDisconnect(conn_test)


