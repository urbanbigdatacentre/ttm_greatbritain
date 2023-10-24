
############################################################################
############################################################################
###                                                                      ###
###         OUTPUT AREA DUCKDB QUERY EXAMPLES AND ISOCHRONE MAPS         ###
###                                                                      ###
############################################################################
############################################################################


# Pacakges ----------------------------------------------------------------

library(DBI)
library(duckdb)
library(tidyverse)
library(sf)


# Supplementary data ------------------------------------------------------

# Define closest OA from main Train station
nearest_oa_code <- 
  c(
    "London" = "E00004667", 
    "Manchester" = "E00176055", 
    "Birmingham" = "E00175644",
    "Slough and Heathrow" = "E00166680",
    "Glasgow" = "S00116383",
    "Newcastle" = "E00175585",
    "Liverpool" = "E00176659",
    "Leicester" = "E00169534",
    "Sheffield" = "E00172508"
  )

# Create a string of from_ids for the SQL query
oa_code_str <- paste0(
  "('", paste(nearest_oa_code, collapse = "','"), "')"
)


# Establish a connection to the DB ----------------------------------------

# Define the DuckDB database file
db_file <- "output/ttm/oa/ttm_oa.duckdb"
# Connect to the DuckDB database
con <- dbConnect(duckdb::duckdb(), db_file)

# Check connection. List of tables
dbListTables(con)


# Operations --------------------------------------------------------------

# Head
dbGetQuery(con, "
    SELECT *
    FROM ttm_pt
    LIMIT 20
  ")

# Number of NAs in 50th and 75th percentile
missing_counts_df <- dbGetQuery(con, "
  SELECT COUNT_IF(travel_time_p50 IS NULL) AS missing_p50,
         COUNT_IF(travel_time_p75 IS NULL) AS missing_p75
  FROM ttm_pt
")
# Print in millions
missing_counts_df / 1e6


# Query and summarize the all the TTM by origin according to different time cuts. 17 sec
system.time(
  cum_summary <- dbGetQuery(con, "
      SELECT from_id,
             COUNT_IF(travel_time_p50 <= 30) AS count_30,
             COUNT_IF(travel_time_p50 <= 60) AS count_60,
             COUNT_IF(travel_time_p50 <= 120) AS count_120,
             COUNT(*) AS count_all
      FROM ttm_pt
      GROUP BY from_id
    ")
)

# Query to extract travel time from a specific OA origin, elapsed 0.08
system.time(
  singleorigin_query <- dbGetQuery(con, "
    SELECT *
    FROM ttm_pt
    WHERE from_id = 'E00004187'
  ")
)

# Query to extract travel times based on a list of OA origins, 19 secs
system.time(
  filtered_query <- dbGetQuery(con, sprintf("
    SELECT *
    FROM ttm_pt
    WHERE from_id IN %s
  ", oa_code_str))
)


# Equivalent queries in tidyverse style -----------------------------------


# Head
tbl(con, "ttm_pt") %>% 
  head() %>% 
  show_query()

# Extract single origin
singleorigin_query <- tbl(con, "ttm_pt") %>% 
  filter(from_id == 'E00004187')

# Extract multiple origins and collect results in R memory
filtered_query <- tbl(con, "ttm_pt") %>% 
  filter(from_id %in% nearest_oa_code) %>% 
  collect()


# Close the SQLite connection
dbDisconnect(con)


# Examine output ----------------------------------------------------------


# summary by OA origin
summary(cum_summary)

# Total OD routes: 4.75 bn
sum(cum_summary$count_all) / 1e9

# Total OD routes estimated by country
cum_summary %>% 
  mutate(
    country = str_sub(from_id, 0, 1),
    country = factor(country, labels = c('England', 'Scotland', 'Wales'))
  ) %>% 
  group_by(country) %>% 
  summarise(total_150 = sum(count_all))


# Isochrone map -----------------------------------------------------------

library(tmap)

# Read output boundaries
oa_geoms <- st_read('data/uk_dataservice/infuse_oa_lyr_2011_clipped/infuse_oa_lyr_2011_clipped.shp')

# Cities as DF
ttwa_selected <- 
  data.frame(
    ttwa11nm = names(nearest_oa_code), 
    geo_code = nearest_oa_code
  )

# Join geoms to origins
filtered_query_sf <- filtered_query %>% 
  filter(!is.na(travel_time_p50)) %>% 
  left_join(oa_geoms, by = c('to_id' = 'geo_code')) %>% 
  left_join(ttwa_selected, by = c('from_id' = 'geo_code')) %>% 
  st_as_sf()

# Map breaks
tt_breaks <- seq(0, 160, by = 20)
# Map palette
tt_palette <- 
  viridisLite::viridis(length(tt_breaks)-1, option = 'turbo', direction = -1)

# Create map
iso_map <-  filtered_query_sf %>% 
  tm_shape() +
  tm_fill(
    col = "travel_time_p50",
    palette = tt_palette, 
    breaks = tt_breaks,
    title = 'Travel time (min)',
    legend.is.portrait = FALSE
  ) +
  tm_scale_bar(position = c(0.05, 0.05), text.size = 0.75, ) +
  tm_facets(by = "ttwa11nm") +
  tm_layout(
    frame = FALSE,
    legend.outside = TRUE,
    legend.outside.position = "bottom",
    legend.outside.size = 0.1
  ) 


# Save map as png
tmap_save(
  iso_map, 
  filename = "plots/isochrone_maps.png", 
  width = 8, height = 10, 
  dpi = 600
)


# Plot cumulative destinations --------------------------------------------


library(ggrepel)
library(scales)

# Plot labels
labs_plot <- filtered_query_sf %>% 
  as.data.frame() %>% 
  mutate(ttwa11nm = gsub(" and ", " and\n", ttwa11nm)) %>% 
  group_by(ttwa11nm) %>% 
  summarise(y = n(), x = 150)

# Cumulative destinations plot
cum_plot <-
  filtered_query_sf %>% 
  as.data.frame() %>% 
  mutate(ttwa11nm = gsub(" and ", " and\n", ttwa11nm)) %>% 
  filter(!is.na(travel_time_p50)) %>% 
  group_by(ttwa11nm) %>% 
  arrange(travel_time_p50) %>% 
  mutate(rn = row_number()) %>% 
  ggplot(aes(travel_time_p50, rn, color = ttwa11nm)) +
  geom_line() +
  coord_cartesian(clip = "off") +
  scale_y_continuous(label = comma, n.breaks = 8) +
  scale_x_continuous(breaks = seq(0, 150, by = 30)) +
  scale_color_viridis_d(option = 'turbo') +
  labs(
    x = "Travel time 50th percentile (min)",
    y = "Number of OAs reacheable"
  ) +
  geom_label_repel(
    data = labs_plot, 
    aes(x, y, label = ttwa11nm), 
    xlim = c(155, 180)
  ) +
  theme_minimal() +
  theme(
    legend.position = 'none',
    plot.margin = unit(c(0.2,4,0.2,0.2), "cm")
  )

# Save plot
ggsave(
  'plots/plot_cumulativeoa.png', 
  cum_plot, 
  width = 8, height = 5, 
  dpi = 300, bg = 'white'
)

