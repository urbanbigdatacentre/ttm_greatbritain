
############################################################################
############################################################################
###                                                                      ###
###                              SECTION 2:                              ###
###                     DEFINE OUTPUT AREA CENTROIDS                     ###
###                                                                      ###
############################################################################
############################################################################

# DATE: 2023-05-16

# This is based on Output Areas. The total in GB should be 46351+181408=227759


# Packages ----------------------------------------------------------------

library(sf)
library(tidyverse)
library(mapview)


# Read data ---------------------------------------------------------------

# Scotland pop. weighted centroids
scot_cent <- st_read("data/scottish_gov/output-area-2011-pwc/OutputArea2011_PWC.shp")
# England pop. weighted centroids
engl_cent <- st_read("data/uk_gov/Output_Areas_Dec_2011_PWC_2022_-2138633279883041547/OA_2011_EW_PWC.shp")


# Merge and save pop. w. centroids ----------------------------------------


# Rename name/code to make compatible
scot_cent <- scot_cent %>%
  rename(
    int_index = OBJECTID,
    geo_code = code,
    geo_label = masterpc
  )
# coordinates to numeric
scot_cent <- scot_cent %>% 
  mutate(
    int_index = as.character(int_index),
    easting = as.numeric(easting),
    northing =  as.numeric(northing)
  )

# Extract coords for England
engl_cent <- engl_cent %>% 
  mutate(
    easting = st_coordinates(.)[,1],
    northing = st_coordinates(.)[,2]
  )
# Rename cols for England
engl_cent <- engl_cent %>%
  rename(
    int_index = GlobalID,
    geo_code = OA11CD
  )


# Bind Scotland and England centroids
gb_cent <- bind_rows(scot_cent, engl_cent)


# Are codes unique?
n_distinct(gb_cent$geo_code)

# Save GB centroids
dir.create("data/centroids")
# As GPKG
st_write(gb_cent, "data/centroids/gb_oa_pwc2011.gpkg")

# Clean env.
rm(list = ls())
gc(reset = TRUE)
