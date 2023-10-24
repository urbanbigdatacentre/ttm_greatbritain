
###########################################################################
###########################################################################
###                                                                     ###
###      LSOA/DZ DUCKDB QUERY EXAMPLES AND NIGHTIME COMPARISON MAP      ###
###                                                                     ###
###########################################################################
###########################################################################

# Date: 2023-07-01

# Load packages -----------------------------------------------------------

library(DBI)
library(tidyverse)
library(duckdb)

# Establish connection ----------------------------------------------------

# Define the database file
db_file <- "output/ttm/lsoa/ttm_lsoa.duckdb"

# Connect to the DuckDB database
con <- dbConnect(duckdb::duckdb(), db_file)

# List of tables in DB
dbListTables(con)


# Extract data from database ----------------------------------------------

# Head
dbGetQuery(con, "SELECT * FROM ttm_pt LIMIT 10")

# Query and summarize the data by origin and time of the day
system.time(
  ttm_summary <- dbGetQuery(con, "
    SELECT from_id, 
           time_of_day, 
           COUNT(*) AS count,
           MEDIAN(travel_time_p50) AS median_travel_time
    FROM ttm_pt
    GROUP BY from_id, time_of_day
  ")
)


# Query to subset specific origins ('from_id'): 20 secs, vs 3 sec
system.time(
  ttm_subset <- dbGetQuery(con, "
    SELECT *
    FROM ttm_pt
    WHERE from_id IN ('S01006526', 'E01000128')
  ")
)


# If enough RAM memory available, extract entire TTM
# Query to fetch all rows and columns from 'ttm_pt_lsoa' table: 3.8 mins, vs 45 secs
system.time(
  ttm_all <- dbGetQuery(con, "
    SELECT *
    FROM ttm_pt
  ")
)


# Close the DB connection
dbDisconnect(con)


# See output --------------------------------------------------------------

library(sf)
library(mapview)
library(ggrepel)


# Structure of subset (raw data)
head(ttm_subset, 10)

# Total OD routes: 265 million. 
sum(ttm_summary$count)

# 163.7 million am, and 100.7 at pm
ttm_summary %>% 
  group_by(time_of_day) %>% 
  summarise(
    total = sum(count),
    average_n_dest = mean(count)
  )


# Explore time of day differences -----------------------------------------

# Read LSOA geoms
lsoa_geoms <- st_read('data/uk_dataservice/infuse_lsoa_lyr_2011/infuse_lsoa_lyr_2011.shp')
# Read LSOA centroids
centroids <- st_read('data/centroids/gb_lsoa_centroid2011.gpkg')
# Read TTWAs
ttwa <- st_read('data/uk_gov/ttwa2011_pop.gpkg')

# Most populated TTWAs
most_populated_ttwa <- ttwa %>% 
  st_drop_geometry() %>% 
  arrange(-total_population) 

# Estimate percent change
dest_n_difference <- ttm_summary %>% 
  pivot_wider(
    names_from = time_of_day, 
    values_from = c(count, median_travel_time)
  ) %>% 
  mutate(
    # NA if number of destinations is too low
    count_am = if_else(count_am <= 5, NA, count_am),
    difference_dest_n = count_pm - count_am,
    difference_dest_pct = (count_pm / count_am - 1) * 100
  )


# Join TTWA data
dest_n_difference <- dest_n_difference %>% 
  left_join(centroids, by = c('from_id' = 'geo_code')) %>% 
  st_as_sf() %>% 
  st_transform(27700) %>% 
  st_join(ttwa) %>% 
  st_drop_geometry()

# # Check raw results
# View(dest_n_difference)



# League table by TTWA -----------------------------------------------------

dest_n_difference %>%
  filter(ttwa11nm %in% most_populated_ttwa$ttwa11nm[1:24]) %>% 
  group_by(ttwa11nm) %>% 
  summarise(
    count = n(),
    # avg_change_n = mean(difference_dest_n, na.rm = TRUE),
    # sd_change_n = sd(difference_dest_n, na.rm = TRUE),
    avg_change_pct = mean(difference_dest_pct,  na.rm = TRUE),
    sd_change_pct = sd(difference_dest_pct,  na.rm = TRUE)
  ) %>% 
  arrange(-avg_change_pct) %>% 
  kableExtra::kbl(digits = 1)


# Map ---------------------------------------------------------------------



# Map breaks and labels
diff_breaks <- c(-100, -75, -50, -25, 40)
diff_labs <- c(
  'Severe reduction \n(-100% to -75%)',
  'Substantial reduction \n(-75% to -50%)',
  'Large reduction \n(-50% to -25%)',
  'Moderate reduction \n(-25% to >0%)'
)
# Legend fill
legend_fill <- 'Morning peak vs. night time: \nPercent change in number of \ndestinations reachable within 2.5 hours'

# City labels
cities <- c("Glasgow", "London", "Sheffield", "Nottingham", "Birmingham")
city_labs <- ttwa %>% 
  arrange(-total_population) %>% 
  filter(grepl(paste(cities, collapse = '|'), ttwa11nm)) %>% 
  st_centroid() %>% 
  mutate(
    x = st_coordinates(.)[,1],
    y = st_coordinates(.)[,2],
    ttwa11nm = sub('and ', '\n', ttwa11nm)
  ) %>% 
  st_drop_geometry()

# Map
map_difference <- dest_n_difference %>% 
  mutate(
    difference_breaks = cut_number(difference_dest_n, 10),
    difference_breaks_pct = cut_number(difference_dest_pct, 10),
    difference_lab_breaks = cut(difference_dest_pct, diff_breaks, labels = diff_labs, include.lowest = TRUE)
  ) %>%
  left_join(lsoa_geoms, by = c('from_id' = 'geo_code')) %>% 
  st_as_sf() %>% 
  ggplot() +
  geom_sf(aes(fill = difference_lab_breaks), col = NA) +
  scale_fill_viridis_d(option = 'rocket', na.value = "grey50", begin = 0.15, end = 0.85) +
  labs(fill = legend_fill) +
  geom_label_repel(
    data = city_labs,
    aes(x, y, label = ttwa11nm), 
    size = 3.2,
    force = 1,
    direction    = "y",
    nudge_x = - 5e5, nudge_y = 500,
    segment.curvature = -1e-20,
    segment.size = 0.7,
    segment.alpha = 0.7,
    alpha = 0.9,
    hjust = 1, 
    xlim = c(20e3),
  )+
  theme_void() +
  theme(
    legend.key.height = unit(1.3, 'cm'), 
    legend.position = c(.95, .65), 
    plot.margin = unit(c(0,0,0,-10), "cm")
  )

# Save map
ggsave(
  'plots/map_difference.png', 
  map_difference, 
  width = 8, height = 11, 
  dpi = 500, bg = 'white'
)


# Interactive map  --------------------------------------------------------

# library(mapview)
# library(leaflet)
# 
# # Initial view centroid
# cent <- c(-0.862071, 52.326242)
# # Map
# int_map <- dest_n_difference %>% 
#   mutate(
#     difference_breaks = cut_number(difference_dest_n, 10),
#     difference_breaks_pct = cut_number(difference_dest_pct, 10),
#     difference_lab_breaks = cut(difference_dest_pct, diff_breaks, labels = diff_labs, include.lowest = TRUE)
#   ) %>% 
#   left_join(lsoa_geoms, by = c('from_id' = 'geo_code')) %>%
#   st_as_sf() %>% 
#   select(name, count_am, count_pm, starts_with('difference')) %>% 
#   mapview(zcol = 'difference_lab_breaks', alpha = 0, layer.name = 'Morning peak vs. night time')
# 
# int_map@map %>% 
#   setView(cent[1], cent[2], zoom = 8)
