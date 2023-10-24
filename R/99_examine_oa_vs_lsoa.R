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

# Libraries ---------------------------------------------------------------

# Allocate RAM to Java
options(java.parameters = "-Xmx110G")
# Load R5R
library(r5r)
library(tidyverse)
library(sf)
library(mapview)


# Read OA centroids
centroids_oa <- st_read("data/centroids/gb_oa_pwc2011.gpkg")
# Transform CRS
centroids_oa <- st_transform(centroids_oa, crs = 4326)
# Rename
centroids_oa <- rename(centroids_oa, id = geo_code)

# Read LSOA/DZ centroids
centroids_lsoa <- st_read("data/centroids/gb_lsoa_centroid2011.gpkg")
# Transform CRS
centroids_lsoa <- st_transform(centroids_lsoa, crs = 4326)
# Rename
centroids_lsoa <- rename(centroids_lsoa, id = geo_code)

# Lookup
lookup <- read_csv('data/uk_gov/Output_Area_Lookup_in_Great_Britain.csv')


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


# Define O-Ds -------------------------------------------------------------

origin_lsoa <- centroids_lsoa %>% 
  filter(id == 'E01031961')

destinations_lsoa <- centroids_lsoa %>% 
  filter(st_intersects(., st_buffer(origin_lsoa, 50e3), sparse = FALSE))



# TT public transport -----------------------------------------------------


itineraties_lsoa <- 
  detailed_itineraries(
    r5r_core = openbusdata_core,
    origins = origin_lsoa,
    destinations = destinations_lsoa,
    mode = mode,
    departure_datetime = departure_datetime,
    # time_window = time_window,
    # percentiles = percentiles,
    max_walk_time = max_walk,
    max_trip_duration = max_trip_duration,
    walk_speed = walk_speed, 
    shortest_path = TRUE
)

# Plot routes
itineraties_test %>% 
  arrange(total_duration) %>% 
  mapview(zcol = 'mode') +
  mapview(destinations_lsoa, col.region ='red') +
  mapview(origins_oa, col.region = 'black') +
  mapview(origin_lsoa, col.region = 'green')



# Define OA Origins-Dest --------------------------------------------------

# OA Origins
origins_oa <- centroids_oa %>% 
  left_join(lookup, by = c('id' = 'OA11CD')) %>% 
  filter(LSOA11CD == 'E01031961')
# OA destinations
destinations_oa <- centroids_oa %>% 
  filter(st_intersects(., st_buffer(origin_lsoa, 50e3), sparse = FALSE))



# OA itineraties ----------------------------------------------------------

itineraties_oa <- 
  detailed_itineraries(
    r5r_core = openbusdata_core,
    origins = origins_oa,
    destinations = destinations_oa,
    mode = mode,
    departure_datetime = departure_datetime,
    max_walk_time = max_walk,
    max_trip_duration = max_trip_duration,
    walk_speed = walk_speed, 
    shortest_path = TRUE, 
    all_to_all = TRUE
  )

# Map
itineraties_oa %>% 
  filter(from_id == 'E00163000') %>% 
  mapview(zcol = 'mode') +
  mapview(destinations_oa, col.region ='red') +
  mapview(origins_oa, col.region = 'black') +
  mapview(origin_lsoa, col.region = 'green')


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
#   [1] mapview_2.11.0  arrow_12.0.1    sf_1.0-12       lubridate_1.9.2 forcats_1.0.0   stringr_1.5.0  
# [7] dplyr_1.1.2     purrr_1.0.1     readr_2.1.4     tidyr_1.3.0     tibble_3.2.1    ggplot2_3.4.2  
# [13] tidyverse_2.0.0 r5r_1.1.0      
# 
# loaded via a namespace (and not attached):
#   [1] tidyselect_1.2.0        farver_2.1.1            fastmap_1.1.1           leaflet_2.1.2          
# [5] duckdb_0.8.1            digest_0.6.31           timechange_0.2.0        lifecycle_1.0.3        
# [9] ellipsis_0.3.2          terra_1.7-29            magrittr_2.0.3          compiler_4.3.0         
# [13] rlang_1.1.1             tools_4.3.0             utf8_1.2.3              yaml_2.3.7             
# [17] data.table_1.14.8       knitr_1.42              htmlwidgets_1.6.2       bit_4.0.5              
# [21] sp_1.6-0                classInt_0.4-9          RColorBrewer_1.1-3      KernSmooth_2.23-20     
# [25] withr_2.5.0             grid_4.3.0              stats4_4.3.0            fansi_1.0.4            
# [29] e1071_1.7-13            leafem_0.2.0            colorspace_2.1-0        scales_1.2.1           
# [33] cli_3.6.1               rmarkdown_2.21          crayon_1.5.2            generics_0.1.3         
# [37] rstudioapi_0.14         tzdb_0.3.0              DBI_1.1.3               proxy_0.4-27           
# [41] parallel_4.3.0          assertthat_0.2.1        s2_1.1.3                base64enc_0.1-3        
# [45] vctrs_0.6.2             webshot_0.5.4           jsonlite_1.8.4          hms_1.1.3              
# [49] bit64_4.0.5             crosstalk_1.2.0         units_0.8-2             glue_1.6.2             
# [53] codetools_0.2-19        leaflet.providers_1.9.0 stringi_1.7.12          rJava_1.0-6            
# [57] gtable_0.3.3            raster_3.6-20           munsell_0.5.0           pillar_1.9.0           
# [61] htmltools_0.5.5         satellite_1.0.4         R6_2.5.1                wk_0.7.2               
# [65] sfheaders_0.4.2         vroom_1.6.3             evaluate_0.20           lattice_0.21-8         
# [69] png_0.1-8               backports_1.4.1         class_7.3-21            Rcpp_1.0.10            
# [73] checkmate_2.2.0         xfun_0.39               pkgconfig_2.0.3        