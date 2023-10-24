 
# PERPARING OSM DATA

# Reference:
# https://docs.conveyal.com/prepare-inputs

# DATE: 2023/05/05
# Requires having osmosis installed
# https://github.com/openstreetmap/osmosis
 
 
# Filtering out unneeded data 
"C:\osmosis\bin\osmosis" \
  --read-pbf "data\osm\great-britain-latest.osm.pbf"  \
  --tf accept-ways highway=* public_transport=platform railway=platform park_ride=* \
  --tf accept-relations type=restriction \
  --used-node \
  --write-pbf "data\osm\great-britain-light.osm.pbf"