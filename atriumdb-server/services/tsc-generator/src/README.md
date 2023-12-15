# Introduction
The TSC (time series compression) generator aggregates the data from the Wal files into TSC files, compresses them and stores meta information in the AtriumDB database.

# Getting Started
The TSC generator interacts with two systems namely the WAL writer and the meta database. The TSC service has several config parameters that need to be set in a config.yaml file so it can interact with those services.

- loglevel str: This sets the logging level. Can be one of ["debug", "info", "warning", "error", "critical"]
- dataset_location str: This sets the location of the tsc and wal folders containing the TSC and WAL files. Also optionally the meta folder which contains the sqlite database file.
- instance_name str: This is the name that specifies this install for open telemetry metrics.

## TSC Generator
- max_workers int: This is the maximum number of worker threads to use for the time series compression.
- default_wait_close_time int: This is the amount of time to wait in seconds before closing a TSC file.
- wait_recheck_time int: This is the amount of time in seconds to wait to check if there are closed WAL files in the wal folder. This stops the TSC generator from pinning a cpu core to 100% if there are no wal files to be aggregated.
- optimal_block_num_values int: This specifies the optimal number of values to put into a single block. The higher this number is the smaller your block_index table will be. However, if you make it too big your read performance will suffer when asking for smaller segments of data since the sdk will have to decompress the entire block.
- create_dataset bool: This specifies if you want the tsc generator to create a dataset at startup. It will not overwrite a dataset if one already exists. This is good for dev if you are constantly needing to restart.
- interval_index_mode str: Determines the mode for writing data to the interval index. Modes include "disable", "fast", and "merge". The default is "merge" and it is recommended to keep it this way. Unless you have really gappy data and it is slowing down the tsc generator too much. For more information on this setting see the write_data function in the Atriumdb docs. 
- metadb_connection str: This is the name of the metadata database connection and should match the one specified in the config.

## Meta Database
This is the backend database that contains all of the information put into AtriumDB. This is neesed so the TSC generator can tell AtriumDB what information is stored in which TSC files. The config parameters to set here are:
- type str: The type of database. Can be one of ["mysql", "sqlite", "mariadb"]
- host str: The host name or IP address of the database. This is not needed if the database is sqlite.
- port int: The port of the database. This is not needed if the database is sqlite.
- username str: Username for the database. This is not needed if the database is sqlite.
- password str: Password for the database. This is not needed if the database is sqlite.
- db_name str: Name of the database. This is not needed if the database is sqlite.

# Docker
This service is deployed using Docker and there are several things to take into account when deploying this service.

## Volume Mapping
First you have to map several volumes. The two that are required are your host tsc folder to /data/tsc and your host wal folder to /data/wal. 
The wal folder is where the TSC generator will grab WAL files and the tsc folder is where your TSC files will sit. Both of these folders should be shared with the WAL writer. 
If you are using an sqlite database you will also have to map your hosts meta folder to /data/meta. You also have to map the config.yaml file on your 
host machine to a file called config.yaml in the container.

## Other Considerations
- The TSC generator has to be networked to the meta database if you are not using sqlite
- The meta database should start up before the TSC generator since it has to connect to it
- This service was meant to be deployed in a docker-compose setup and an example compose file can be found in the repository