############################################################################
############################################################################
###                                                                      ###
###                     COMPUTE TTM AT LSOA/DZ LEVEL                     ###
###                                                                      ###
############################################################################
############################################################################


# Date: 2023-06-25
# R version 4.3.0 (2023-04-21 ucrt)
# Author: J Rafael Verduzco

# This code:
# Estimate the LSOA/DZ TTM by chunks
# The result are appended into a table in a DuckDB 
# The TTM table is exported into a parquet file

# Libraries ---------------------------------------------------------------

# Allocate RAM to Java
options(java.parameters = "-Xmx110G")
# Load R5R, V 1.0.1
library(r5r)

library(tidyverse)
library(data.table)
library(sf)
library(duckdb)

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
mode <- c("WALK", "TRANSIT")
# Max trip duration
max_trip_duration <- 150L
# Max walking distance
# As default:  no restrictions as long as max_trip_duration is respected
max_walk <- Inf
# Walk speed
walk_speed <- 4.8
# Tuesday 7 to 10 am and 9 to 12 pm
departure_datetime <- c(am = "2023-03-07 07:00:00", pm = "2023-03-07 21:00:00")
departure_datetime <- as.POSIXct(departure_datetime)
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
dir.create('output/ttm/lsoa/', recursive = TRUE)

# Define the DuckDB database file
db_file <- "output/ttm/lsoa/ttm_lsoa.duckdb"


# Batch size
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
log_file <- file("output/ttm/lsoa/ttm_log.txt", "w")

# Connect to the DuckDB database
con <- dbConnect(duckdb::duckdb(), db_file)

# Times of the day to compute TTM
times_day <- names(departure_datetime)

# Run loop for time of the day
start_time <- Sys.time()
for (t in times_day){
  # Run loop in batches
  for (i in seq_along(origin_batches)) {
    loop_start_time <- Sys.time()  # Start time for the current loop
    
    # Progress
    cat(
      paste(round(Sys.time()), "- TOD: ", t, "- Batch", i, "of", length(origin_batches)), "\n",
      file = log_file, append = TRUE
    )
    
    # Calculate ttm
    ttm_pt <- travel_time_matrix(
      r5r_core = openbusdata_core,
      origins = origin_batches[[i]],
      destinations = centroids,
      mode = mode,
      departure_datetime = departure_datetime[t],
      time_window = time_window,
      percentiles = percentiles,
      max_walk_time = max_walk,
      max_trip_duration = max_trip_duration,
      walk_speed = walk_speed
    )
    
    # Add time of the day
    ttm_pt[,time_of_day := t]
    
    # Append ttm_pt to the DuckDB database
    dbWriteTable(con, name = 'ttm_pt', value = ttm_pt, append = TRUE, row.names = FALSE)
    
    # Get the number of rows in ttm_pt
    num_rows <- nrow(ttm_pt)
    
    # Write number of rows and computation time to the log file
    cat(paste("Number of rows:", num_rows), "\n",
        file = log_file, append = TRUE)
    
    loop_end_time <- Sys.time()  # End time for the current loop
    cat(paste("Loop computation time:", round(as.numeric(difftime(loop_end_time, loop_start_time, units = "secs")), 2), "secs"), "\n",
        file = log_file, append = TRUE)
    
    # Clean env
    rm(ttm_pt)
    gc()
  }
}
end_time <- Sys.time()

# Write total computation time to the log file
cat(paste("Total computation time:", round(as.numeric(difftime(end_time, start_time, units = "secs")), 2), "secs"), "\n",
    file = log_file, append = TRUE)

# Close the log file
close(log_file)


# Export TTM to parquet ---------------------------------------------------

# Out directory
out_dir <- 'output/ttm/lsoa/ttm_pt/'
dir.create(out_dir)

# Get full TTM from DB
ttm_lsoa <- dbGetQuery(con, "FROM ttm_pt ORDER BY ALL")

# Write TTM as parquet
arrow::write_dataset(
  dataset = ttm_lsoa, 
  path = out_dir, 
  format = 'parquet', 
  max_rows_per_file = 3e6
)

# Close the DuckDB connection
dbDisconnect(con, shutdown=TRUE)


# Test reading TTM --------------------------------------------------------

conn_test <- dbConnect(duckdb::duckdb())

ttm_test <- dbGetQuery(conn_test, "
     SELECT * 
     FROM read_parquet(['output/ttm/lsoa/ttm_lsoa/*.parquet'])
     ORDER BY ALL;
")

all.equal(ttm_lsoa, ttm_test)

