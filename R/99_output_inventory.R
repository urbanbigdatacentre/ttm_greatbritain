# Data Package Manifest

library(fs)
library(tidyverse)

# Directories to exclude
dir_exclude <- c("comparison|ttm_log|manifest")

# Create directory info
package_manifest <- dir_info(path = 'output', recurse = TRUE) %>% 
  select(path, type, size) %>% 
  filter(!grepl(dir_exclude, path))

sum(package_manifest$size)

# Save as csv
write_csv(package_manifest, 'output/ttm/package_inventory.csv')
