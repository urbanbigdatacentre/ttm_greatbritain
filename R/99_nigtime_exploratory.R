
# LSOA/DZ Explore nighttime differences

# Load packages -----------------------------------------------------------

library(DBI)
library(duckdb)
library(sf)
library(ggrepel)
library(tidyverse)

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
ttm_summary_25 <- dbGetQuery(con, "
  SELECT from_id, 
         time_of_day, 
         COUNT_IF(travel_time_p75 <= 30) AS count_30,
         COUNT_IF(travel_time_p75 <= 60) AS count_60,
         COUNT_IF(travel_time_p75 <= 90) AS count_90,
         COUNT_IF(travel_time_p75 <= 120) AS count_120,
         COUNT(*) AS count_150
  FROM ttm_pt
  WHERE travel_time_p25 IS NOT NULL
  GROUP BY from_id, time_of_day
")

sum(ttm_summary_25$count_150)

# Query and summarize the data by origin and time of the day
ttm_summary <- dbGetQuery(con, "
  SELECT from_id, 
         time_of_day, 
         COUNT_IF(travel_time_p75 <= 30) AS count_30,
         COUNT_IF(travel_time_p75 <= 60) AS count_60,
         COUNT_IF(travel_time_p75 <= 90) AS count_90,
         COUNT_IF(travel_time_p75 <= 120) AS count_120,
         COUNT(*) AS count_150
  FROM ttm_pt
  WHERE travel_time_p50 IS NOT NULL
  GROUP BY from_id, time_of_day
")

sum(ttm_summary$count_150)

# Close the DB connection
dbDisconnect(con)


# Compute changes ---------------------------------------------------------

# Restructure TTM data
ttm_summary_all <- ttm_summary_25 %>% 
  pivot_longer(
    c(-from_id, -time_of_day), 
    names_to = "timecut", 
    values_to = "count_p50"
  ) %>% 
  mutate(timecut = parse_number(timecut)) %>% 
  pivot_wider(names_from = time_of_day, values_from = count_p50)

# Compute change in number destinations reachable
ttm_summary_all <- ttm_summary_all %>% 
  mutate(
    am = replace(am, am <= 3, NA),
    difference_dest_n = pm - am, 
    difference_dest_pct = difference_dest_n / am * 100
  )

# Remove 150 min for now
ttm_summary_all <- ttm_summary_all %>%
  filter(timecut != 150)


# Read secondary data -----------------------------------------------------

# Read LSOA geoms
lsoa_geoms <- st_read('data/uk_dataservice/infuse_lsoa_lyr_2011/infuse_lsoa_lyr_2011.shp')
# Centroids
centroids <- st_read('data/centroids/gb_lsoa_centroid2011.gpkg')
# Read TTWAs
ttwa <- st_read('data/uk_gov/ttwa2011_pop.gpkg')
# Vlookup file
lookup <- read_csv('data/uk_gov/Output_Area_Lookup_in_Great_Britain.csv')
# Residential-based classification hierachy
residential_class <- 
  readxl::read_xls(
    'data/ons/residential_calssifications/clustermembershipv2.082018.xls', 
    sheet = 1, 
    skip = 3
)


# Format data -------------------------------------------------------------

# Residential classification 
residential_class <- residential_class %>% 
  janitor::clean_names() %>%
  slice(1:24) %>% 
  zoo::na.locf()

# Limit lookup using relevant features
lookup <- lookup %>% 
  select(LSOA11CD, LSOA11NM, SOAC11NM, LACNM, RGN11NM, CTRY11NM) %>% 
  janitor::clean_names() %>%
  distinct()

# Most populated cities
ttwa <- ttwa %>% 
  arrange(-total_population) %>% 
  mutate(
    pop_rank = 1:nrow(.), 
    larger_city = if_else(pop_rank > 24, 'Other', ttwa11nm)
  )

# Join TTWA data to LSOAs
centroids <- select(centroids, geo_code)
ttwa <- select(ttwa, -globalid, -pop_rank)
ttwa_at_lsoa <- st_join(centroids, ttwa)  %>% 
  st_drop_geometry()

# Join data
ttm_summary_all <- ttm_summary_all %>%
  # Join InFuse data
  left_join(lookup, by = c('from_id' = 'lsoa11cd')) %>% 
  # Join TTWA data
  left_join(ttwa_at_lsoa, by = c('from_id' = 'geo_code')) %>% 
  # Join classification hierarchy
  left_join(residential_class, by = c('soac11nm' = 'group_name'))


# Distribution plots ------------------------------------------------------


# Histrogram by timecut
ttm_summary_all %>% 
  ggplot(aes(difference_dest_pct)) +
  geom_histogram() +
  geom_vline(xintercept = 0, col = 'orange', lwd = 1) +
  facet_wrap(~timecut) +
  theme_minimal() 

# Distribution histogram: REGION
ttm_summary_all %>% 
  ggplot(
    aes(
      difference_dest_pct,
      fct_reorder(rgn11nm, difference_dest_pct), 
      fill = factor(timecut)
    )
  ) +
  geom_boxplot() +
  theme_minimal()

# Distribution histogram: Larger cities
ttm_summary_all %>% 
  ggplot(
    aes(
      difference_dest_pct,
      fct_reorder(larger_city, difference_dest_pct), 
      fill = factor(timecut)
    )
  ) +
  geom_boxplot(outlier.alpha = 0.5, outlier.size = 1) +
  theme_minimal()

# Distribution histogram: Local Area Classification
ttm_summary_all %>% 
  ggplot(
    aes(
      difference_dest_pct,
      fct_reorder(lacnm, difference_dest_pct), 
      fill = factor(timecut)
    )
  ) +
  geom_boxplot() +
  theme_minimal()

# Distribution histogram: Super output area classification
ttm_summary_all %>% 
  ggplot(
    aes(
      difference_dest_pct,
      fct_reorder(soac11nm, difference_dest_pct), 
      fill = factor(timecut)
    )
  ) +
  geom_boxplot(outlier.alpha = 0.5, outlier.size = 1) +
  theme_minimal()

# Distribution histogram: Super output area classification
ttm_summary_all %>% 
  ggplot(
    aes(
      difference_dest_pct,
      fct_reorder(supergroup_name, difference_dest_pct), 
      fill = factor(timecut)
    )
  ) +
  geom_boxplot() +
  theme_minimal()

# Check differences in a LM
ttm_summary_all %>% 
  mutate(
    supergroup_name = fct_relevel(supergroup_name, "Inner city cosmopolitan"),
    timecut = factor(timecut)
  ) %>% 
  lm(difference_dest_pct ~ supergroup_name * timecut, .) %>% 
  summary()


# Map ---------------------------------------------------------------------


# Map breaks and labels
diff_breaks <- c(-100, -75, -50, -25, 0, max(ttm_summary_all$difference_dest_pct, na.rm = TRUE))
diff_labs <- c(
  'Severe reduction \n(-100% to -75%)',
  'Substantial reduction \n(-75% to -50%)',
  'Large reduction \n(-50% to -25%)',
  'Moderate reduction \n(-25% to >0%)',
  'Positive'
)

# # City labels
# cities <- c("Glasgow", "London", "Sheffield", "Nottingham", "Birmingham")
# city_labs <- ttwa %>%
#   arrange(-total_population) %>%
#   filter(grepl(paste(cities, collapse = '|'), ttwa11nm)) %>%
#   st_centroid() %>%
#   mutate(
#     x = st_coordinates(.)[,1],
#     y = st_coordinates(.)[,2],
#     ttwa11nm = sub('and ', '\n', ttwa11nm)
#   ) %>%
#   st_drop_geometry()

# map_facet <- ttm_summary_all %>% 
#   mutate(
#     difference_lab_breaks = cut(difference_dest_pct, diff_breaks, labels = diff_labs, include.lowest = TRUE),
#     timecut = factor(timecut) 
#   ) %>%
#   left_join(lsoa_geoms, by = c('from_id' = 'geo_code')) %>% 
#   st_as_sf() %>% 
#   ggplot() +
#   geom_sf(aes(fill = difference_lab_breaks), col = NA) +
#   scale_fill_viridis_d(option = 'rocket', na.value = "grey50", begin = 0.15, end = 0.85) +
#   labs(fill = 'Morning peak vs. night time \nPercent change in \nnumber of destinations reachable') +
#   facet_wrap(~timecut, ncol = 2) +
#   theme_void() 

map_facet <- ttm_summary_all %>%
  mutate(
    difference_lab_breaks = cut(difference_dest_pct, diff_breaks, labels = diff_labs, include.lowest = TRUE),
    difference_lab_breaks = replace_na(),
    timecut = factor(timecut)
  ) %>%
  left_join(lsoa_geoms, by = c('from_id' = 'geo_code')) %>%
  st_as_sf() %>%
  ggplot() +
  geom_sf(aes(fill = difference_lab_breaks), col = NA) +
  scale_fill_viridis_d(option = 'rocket', na.value = "grey70", begin = 0.15, end = 0.85) +
  labs(fill = 'Morning peak vs. night time \nPercent change in \nnumber of destinations reachable') +
  facet_wrap(~timecut, ncol = 2) +
  theme_void()

# Save map
ggsave(
  'plots/map_difference_all.png', 
  map_facet, 
  width = 11, height = 11, 
  dpi = 200, bg = 'white'
)
