############################################################################
############################################################################
###                                                                      ###
###                   COMPUTE TTM AT OUTPUT AREA LEVEL                   ###
###                                                                      ###
############################################################################
############################################################################


# Date: 2023-06-26
# Author: J Rafael Verduzco

# This code:
# Estimates a travel time matrix from each output area in GB to every other
# It brakes the OA centroids and computes the TTM in chunks
# The results are written as parquet files
# Parquet files are consolidated in a DuckDB


# Libraries ---------------------------------------------------------------

# Allocate RAM to Java
options(java.parameters = "-Xmx110G")
# Load R5R
library(r5r)
library(tidyverse)
library(sf)
library(arrow)

# Read OA centroids
centroids <- st_read("data/centroids/gb_oa_pwc2011.gpkg")
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
# Tuesday 7 am
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

# Define the Parquet dataset directory
parquet_dir <- "output/ttm/oa/ttm_oa/"

# Create folder
dir.create(parquet_dir, recursive = TRUE)

# Batch size
batch_size <- 150

# Split origin into batches
origin_batches <- split(
  x = centroids,
  f = rep(
    1:ceiling(nrow(centroids) / batch_size),
    each = batch_size,
    length.out = nrow(centroids)
  )
)

# Log file
log_filepath <- "output/ttm/oa/ttm_log.txt"
if(file.exists(log_filepath)){
  # Open the log file for writing
  log_file <- open(log_filepath, "a")
} else {
  # Open the log file for writing
  log_file <- file(log_filepath, "w")
}

# Run loop in batches
start_time <- Sys.time()
for (i in 1:length(origin_batches)) {
  # Start time for the current loop
  loop_start_time <- Sys.time() 
  
  # Progress
  cat(
    paste(round(Sys.time()), "- Batch", i, "of", length(origin_batches)), "\n",
    file = log_file, append = TRUE
  )
 
  # Calculate ttm
  ttm_pt <- travel_time_matrix(
    r5r_core = openbusdata_core,
    origins = origin_batches[[i]],
    destinations = centroids,
    mode = mode,
    departure_datetime = departure_datetime,
    time_window = time_window,
    percentiles = percentiles,
    max_walk_time = max_walk,
    max_trip_duration = max_trip_duration,
    walk_speed = walk_speed
  )
  
  
  # Append ttm_pt to the Parquet dataset
  parquet_file <- file.path(parquet_dir, paste0("ttm_part-", i, ".parquet"))
  arrow::write_parquet(ttm_pt, parquet_file)
  
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
end_time <- Sys.time()

# Write total computation time to the log file
cat(paste("Total computation time:", round(as.numeric(difftime(end_time, start_time, units = "secs")), 2), "secs"), "\n",
    file = log_file, append = TRUE)

# Close the log file
close(log_file)



# Copy parquet to table in DuckDB -----------------------------------------

library(DBI)
library(duckdb)

# Define the DuckDB database file
db_file <- "output/ttm/oa/ttm_oa.duckdb"
# Connect to the DuckDB database
con <- dbConnect(duckdb::duckdb(), db_file)

# Read from parquet
dbExecute(
  con, 
  "CREATE TABLE ttm_pt AS 
     SELECT * 
     FROM read_parquet(['output/ttm/oa/ttm_oa/*.parquet']);"
)


# Session info ------------------------------------------------------------


# > sessionInfo()
# R version 4.3.0 (2023-04-21 ucrt)
# Platform: x86_64-w64-mingw32/x64 (64-bit)
# Running under: Windows 10 x64 (build 19044)
# 
# Matrix products: default
# 
# 
# locale:
#   [1] LC_COLLATE=English_United Kingdom.utf8  LC_CTYPE=English_United Kingdom.utf8
# [3] LC_MONETARY=English_United Kingdom.utf8 LC_NUMERIC=C
# [5] LC_TIME=English_United Kingdom.utf8
# 
# time zone: Europe/London
# tzcode source: internal
# 
# attached base packages:
#   [1] stats     graphics  grDevices utils     datasets  methods   base
# 
# other attached packages:
#   [1] arrow_12.0.1    sf_1.0-12       lubridate_1.9.2 forcats_1.0.0   stringr_1.5.0
# [6] dplyr_1.1.2     purrr_1.0.1     readr_2.1.4     tidyr_1.3.0     tibble_3.2.1
# [11] ggplot2_3.4.2   tidyverse_2.0.0 r5r_1.0.1
# 
# loaded via a namespace (and not attached):
#   [1] bit_4.0.5          gtable_0.3.3       compiler_4.3.0     tidyselect_1.2.0   Rcpp_1.0.10
# [6] assertthat_0.2.1   scales_1.2.1       R6_2.5.1           generics_0.1.3     classInt_0.4-9
# [11] units_0.8-2        munsell_0.5.0      DBI_1.1.3          pillar_1.9.0       tzdb_0.3.0
# [16] rlang_1.1.1        utf8_1.2.3         stringi_1.7.12     bit64_4.0.5        timechange_0.2.0
# [21] cli_3.6.1          withr_2.5.0        magrittr_2.0.3     class_7.3-21       grid_4.3.0
# [26] rstudioapi_0.14    hms_1.1.3          lifecycle_1.0.3    vctrs_0.6.2        KernSmooth_2.23-20
# [31] proxy_0.4-27       glue_1.6.2         data.table_1.14.8  e1071_1.7-13       fansi_1.0.4
# [36] colorspace_2.1-0   tools_4.3.0        pkgconfig_2.0.3