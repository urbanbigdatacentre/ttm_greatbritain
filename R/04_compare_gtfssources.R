
############################################################################
############################################################################
###                                                                      ###
###       COMPARE TIMETABLE FEED SOURCES: OPENBUSDATA VS TREVELINE       ###
###                                                                      ###
############################################################################
############################################################################

# DATE: 2023-06-24


# Libraries ---------------------------------------------------------------

# Requires JDK version is 11
# Allocate RAM to Java 
options(java.parameters = "-Xmx100G")
# Load R5R, uses V 1.0.1!
library(r5r)

library(tidyverse)
library(gtfstools)
library(ggpubr)


# Read GTFS data ----------------------------------------------------------

# Traveline individual feeds
traveline_dirs <-
  list.files(
    "data/timetable/traveline/20230307/gtfs/",
    pattern = "zip",
    recursive = TRUE,
    full.names = TRUE
  )
traveline_dirs <- traveline_dirs[!grepl("prelim|all", traveline_dirs)]
# Read traveline files
traveline_list <- lapply(traveline_dirs, read_gtfs)
names(traveline_list) <- sub(".gtfs\\.zip", '', basename(traveline_dirs))

# # Merged traveline feeds
# traveline_path <- 'data/timetable/traveline/.../gtfs/all/all.gtfs.zip'
# traveline <- read_gtfs(traveline_path)

# Read openbusdata files
busopendata_dir <- 'data/timetable/busopendata/20230307/bods_20230307.gtfs.zip'
busopendata <- read_gtfs(busopendata_dir)


# Traveline separated feed ------------------------------------------------

# Merge Traveline routes
traveline_routes <- traveline_list %>% 
  map('routes') %>% 
  bind_rows(.id = 'region') 
# Merge Traveline agencies
traveline_agency <- traveline_list %>% 
  map('agency') %>% 
  bind_rows(.id = 'region')

# Show duplicated agency names across regions
traveline_routes %>% 
  left_join(traveline_agency, by = c('agency_id', 'region')) %>% 
  count(region, agency_id) %>% 
  count(agency_id, name = 'feeds_dup_agency') %>% 
  arrange(-feeds_dup_agency)  %>% 
  filter(feeds_dup_agency > 1)

# Unique number of routes provided by different agency, 
# considering  a duplicated agency in a different region provides a diff service
traveline_routes %>% 
  left_join(traveline_agency, by = c('agency_id', 'region')) %>% 
  select(region, agency_id, route_id) %>% 
  n_distinct()
# Now, considering identical agenciy names across diff. regions are the same
traveline_routes %>% 
  left_join(traveline_agency, by = c('agency_id', 'region')) %>% 
  select(agency_id, route_id) %>% 
  n_distinct()

# Compare services --------------------------------------------------------

# # Traveline merged
# # The merge in uk2gtfs keeps duplicated agencies and routes as separated if they come from different files
# traveline$routes %>% 
#   left_join(traveline$agency, by = 'agency_id') %>% 
#   select(agency_id, route_id) %>% 
#   n_distinct()

# BUS Open data
# Unique number of routes provided by different agency
busopendata$routes %>% 
  left_join(busopendata$agency, by = 'agency_id') %>% 
  select(agency_id, route_id) %>% 
  n_distinct()

##---------------------------------------------------------------
##                 Compare in routes estimated                 --
##---------------------------------------------------------------

# Copy input files --------------------------------------------------------

# Create folder for routers
dir.create('router')
# Traveline
traveline_router <- 'router/traveline/'
dir.create(traveline_router)
# Busopendata
busopendata_router <- 'router/busopendata/'
dir.create(busopendata_router)


# List of input files
osm_path <- 'data/osm/great-britain-light.osm.pbf'
atoc_files <- 'data/timetable/atoc/ttis662.gtfs.zip'

# Traveline files
traveline_files <- c(osm_path, atoc_files, traveline_dirs)
# Copy input files
lapply(traveline_files, function(x)
  file.copy(x, paste0(traveline_router, '/', basename(x)))
)

# Busopendata files
busopendata_files <- c(osm_path, atoc_files, busopendata_dir)
# Copy input files
lapply(busopendata_files, function(x)
  file.copy(x, paste0(busopendata_router, '/', basename(x)))
)


# Build network  ----------------------------------------------------------

# Build Traveline network
traveline_core <- setup_r5(data_path = traveline_router, verbose = TRUE)
# Busopendata network
busopendata_core <- setup_r5(data_path = busopendata_router, verbose = TRUE)


# Routing test ------------------------------------------------------------

library(mapview)
library(sf)

# Read OD points
centroids <- st_read('data/centroids/gb_lsoa_centroid2011.gpkg')
centroids <- st_transform(centroids, 4326)
centroids <- rename(centroids, id = geo_code)

# Exclude IOM
traveline_list <- traveline_list[!names(traveline_list) == "iom"]

# Sample size
sample_size <- 1000 # 1000

# Define origins pulling random stops in each region
set.seed(3)
origins_test <- traveline_list %>% 
  map('stops') %>% 
  map(~sample_n(.x, min(nrow(.), sample_size))) %>% 
  map(rename, id = stop_id) %>% 
  map(st_as_sf, coords = c('stop_lon', 'stop_lat'), crs = 4326)
  

# Define destinations pulling random stops stops in regions
set.seed(50)
destinations_test <- traveline_list %>% 
  map('stops') %>% 
  map(~sample_n(.x, min(nrow(.), sample_size))) %>% 
  map(rename, id = stop_id) %>% 
  map(st_as_sf, coords = c('stop_lon', 'stop_lat'), crs = 4326)

# Routing parameters
mode <- c('TRANSIT')
dep_time <- as.POSIXct(
  c(bod = "2023-03-07 12:00:00", tvl = "2023-03-07 12:00:00")
)
max_duration <- 180
mx_rides <- 3
time_window <- 30


# BOD: Run routing test for each region
busopendata_test <- 
  map2(
    .x = origins_test, 
    .y = destinations_test, 
    .f = ~ detailed_itineraries(
      r5r_core = busopendata_core, 
      origins = .x, 
      destinations = .y, 
      mode = mode, 
      departure_datetime = dep_time['bod'], 
      max_trip_duration = max_duration, 
      max_rides = mx_rides,
      time_window = time_window,
      shortest_path = TRUE 
    )
  )

# Traveline: Run routing test for each region
traveline_test <- 
  map2(
    .x = origins_test, 
    .y = destinations_test, 
    .f = ~ detailed_itineraries(
      r5r_core = traveline_core, 
      origins = .x, 
      destinations = .y, 
      mode = mode, 
      departure_datetime = dep_time['tvl'], 
      max_trip_duration = max_duration, 
      max_rides = mx_rides,
      time_window = time_window,
      shortest_path = TRUE
    )
  )

# Bind lists
routing_test <- list(busopendata_test, traveline_test) %>% 
  set_names(c('busopendata', 'traveline')) %>% 
  map(bind_rows, .id = 'region') %>% 
  bind_rows(.id = 'source')

# Write data
dir.create('output/router_comparison/', recursive = TRUE)
saveRDS(routing_test, 'output/router_comparison/estimated_tt.V2.rds')


##----------------------------------------------------------------
##                   Examine routes estimated                   --
##----------------------------------------------------------------

# Read TT data
routing_test <- readRDS('output/router_comparison/estimated_tt.V2.rds')

routing_test <- routing_test %>% 
  mutate(source = factor(source, labels = c('OBDS', 'TNDS')))

# Summarise data
test_summary <- routing_test %>% 
  as.data.frame() %>% 
  group_by(source, region, from_id, to_id) %>% 
  summarise(
    total_duration = first(total_duration), 
    route = paste(route, collapse = '|'),
    n_legs = n()
  ) %>% 
  ungroup()
# Re-shape in wide format
test_wide <- test_summary %>% 
  pivot_wider(names_from = 'source', values_from = c(total_duration, route, n_legs))
summary(test_wide)
# Compute difference
test_wide <- test_wide %>% 
  mutate(tt_difference = total_duration_OBDS - total_duration_TNDS)


# Missing trips -----------------------------------------------------------


# Missing estimated routes summary
test_wide %>% 
  count(
    bod_na = is.na(total_duration_OBDS), 
    tvl_na = is.na(total_duration_TNDS), 
    name = 'trips'
  ) %>% 
  mutate(
    category = case_when(
      bod_na == FALSE & tvl_na == FALSE ~ 'Present in both sources',
      bod_na == FALSE & tvl_na == TRUE ~ 'Missing from Traveline',
      bod_na == TRUE & tvl_na == FALSE ~ 'Missing from OBD'
    ),
    percent = (trips / sum(trips)) * 100
  ) %>% 
  select(category, trips, percent)

# Number of estimated journeys
count_journeys <- test_summary %>% 
  ggplot(aes(y= fct_infreq(region), fill = source)) +
  geom_bar(position = 'dodge') +
  scale_fill_viridis_d(alpha = 0.75, option = 'turbo', begin = .1, end = .9) +
  labs(
    title = 'Timetable feed comparison',
    x = 'Number of journeys estimated within 180 min.',
    y = 'Region',
    fill = 'Feed source'
  ) +
  theme_minimal() +
  theme(legend.position = 'bottom')

# Save PNG
ggsave(
  'plots/feed_compare_count.png', 
  count_journeys, 
  height = 6, width = 9,
  dpi = 300, bg= 'white'
)

# Proportion of trips estimated by source
test_summary %>% 
  ggplot(aes(y= fct_infreq(region), fill = source)) +
  geom_bar(position = 'fill')  +
  scale_fill_viridis_d(alpha = 0.75, option = 'turbo', begin = .1, end = .9) +
  theme_minimal() +
  theme(legend.position = 'bottom')


# Travel time difference --------------------------------------------------

# Density plot
test_wide %>% 
  ggplot(aes(tt_difference)) +
  geom_density(adjust = 2, lwd = 1) +
  geom_vline(xintercept = 0, col = 'orange') +
  labs(
    title = 'Travel time difference (ref. Traveline)',
    x= 'Difference (minutes)'
  ) +
  theme_minimal() 

# Scatter plot: travel differences
compare_estimates <- test_wide %>% 
  mutate(walk_only = if_else(n_legs_traveline < 2, 'Walk only', 'Walk and PT')) %>% 
  ggplot(aes(total_duration_busopendata, total_duration_traveline)) +
  geom_point(alpha = 0.7, aes(col = walk_only)) +
  geom_smooth(col = 'gray20') +
  facet_wrap(~region) +
  scale_color_viridis_d(alpha = 0.75, option = 'turbo', begin = .1, end = .9) +
  labs(
    x = 'Travel time BusOpenData (minutes)',
    y = 'Travel time Traveline (minutes)',
    col = 'Traveline mode'
  ) +
  stat_cor(
    aes(label = paste(..rr.label.., sep = "~`,`~")), # adds R^2 and p-value
    r.accuracy = 0.01,
    p.accuracy = 0.001,
    label.x = 0, label.y = 180, size = 2.75, 
    geom = "label", 
    alpha = 0.7,
    label.r = unit(0.30, "lines"),
  ) +
  theme_minimal() +
  theme(legend.position = 'bottom')

# Save PNG
ggsave(
  'plots/feed_compare_tt.png', 
  compare_estimates, 
  height = 6, width = 9,
  dpi = 300, bg= 'white'
)

# Test OBD on different dates of departure --------------------------------


library(lubridate)

# Define 'exact' date
exact_date <- ymd_hms("2023-03-07 12:00:00")
# Compute other dates
dep_date <- c(
  exact = exact_date,
  one_week_before = exact_date - weeks(1),
  two_weeks_before = exact_date - weeks(2),
  one_week_after = exact_date + weeks(1),
  two_weeks_after = exact_date + weeks(2)
)


# BOD: Run routing test for each region
busopendata_test2 <- 
  # Iterate over date of departure
  purrr::map(
    dep_date, function(dep_date) {
      # Iterate over regions
      purrr::map2(
        .x = origins_test, 
        .y = destinations_test, 
        .f = ~ travel_time_matrix(
          r5r_core = busopendata_core, 
          origins = .x, 
          destinations = .y, 
          mode = mode, 
          departure_datetime = dep_date, 
          max_trip_duration = max_duration, 
          max_rides = mx_rides,
          time_window = time_window
        )
      )
  })

# Merge data
busopendata_test2 <- busopendata_test2 %>%
  map(bind_rows, .id = 'region') %>% 
  bind_rows(.id = 'date_departure')

# Number of journeys estimated
busopendata_test2 %>% 
  count(region, date_departure)
# Plot journeys estimated
busopendata_test2 %>% 
  ggplot(aes(region, fill = date_departure)) +
  geom_bar(position = 'dodge')
