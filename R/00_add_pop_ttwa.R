
# Include total population to 2011 TTWA geometries 


# Date: 2023-06-15

# Packages
library(sf)
library(tidyverse)

# Read data ---------------------------------------------------------------


# TTWA geoms
ttwa <- st_read('data/uk_gov/Travel_to_Work_Areas_Dec_2011/TTWA_2011_UK_BFE_V3.shp')
# Read LSOA centroids
centroids <- st_read("data/centroids/gb_lsoa_centroid2011.gpkg")
# Read Population
population_gb <- read_csv('data/population/population_gb.csv')


# Join population to TTWA -------------------------------------------------

# Col names tolower
ttwa <- ttwa %>% 
  rename_with(tolower, everything())

# Join population figures to centroids
centroids <- 
  left_join(centroids, population_gb, by = c('geo_code' = 'datazone2011code'))
# Spatially join TTWA to centroids
centroids <- st_join(centroids, ttwa)

# Summarise population at TTWA
pop_ttwa <- centroids %>% 
  as.data.frame() %>% 
  group_by(ttwa11cd) %>% 
  summarise(total_population = sum(total_population))

# Join population to TTWA
ttwa <- ttwa %>% 
  left_join(pop_ttwa, by = 'ttwa11cd')

# Save ttwa including population
st_write(ttwa, 'data/uk_gov/ttwa2011_pop.gpkg')
