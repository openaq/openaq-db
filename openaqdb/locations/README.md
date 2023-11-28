# Locations
Locations will consist of a few tables used to store/cache data as needed
## sensor_nodes
Standard data table used to store the location information. Data will be inserted/updated primarilly by the ingest process.
## locations_rollups
Standard data table used to cache summary information for the sensor_nodes. Data will be inserted/updated by the ingest process.
## locations_view
A saved view used to get location information for the locations endpoint
## locations_mv
A materialized view used as a cache for the `locations_view`
## update_locations_rollups
A function used by the ingest process to update the rollups. The updates will be done using the staging tables. Function should lock down and compare rows before updating to prevent race conditions.


## sensor_node_spatial_rollup
For each node we will record
