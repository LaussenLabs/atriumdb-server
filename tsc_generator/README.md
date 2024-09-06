# Introduction
The TSC (time series compression) generator aggregates the data from the Wal files into TSC files, compresses them and stores meta information in the AtriumDB database.
It also optimizes the dataset once a day by combining small TSC files created during ingest into bigger files.

# Getting Started
The TSC generator interacts with two systems namely the WAL writer and the meta database. The TSC service has several config parameters that need to be set in a config.yaml file so it can interact with those services.

- loglevel str: This sets the logging level. Can be one of ["debug", "info", "warning", "error", "critical"]
- dataset_location str: This sets the location of the tsc and wal folders containing the TSC and WAL files. Also optionally the meta folder which contains the sqlite database file.
- instance_name str: This is the name that specifies this install for open telemetry metrics.

## TSC Generator
- max_workers int: This is how many sdk instances (processes) you want to spawn at a time. Each process can work on 1 wal file at a time.
- num_compression_threads int: This is how many C compression threads (for compressing the data) you want to give to each sdk worker. num_workers * num_compression threads should not be more than how many cores you have allocated to docker. Be careful about how many workers you spawn because each one creates a mariadb connection and too many can overwhelm mariadb.
- default_wait_close_time int: This is the amount of time to wait in seconds before closing a TSC file.
- wait_recheck_time int: This is the amount of time in seconds to wait to check if there are closed WAL files in the wal folder. This stops the TSC generator from pinning a cpu core to 100% if there are no wal files to be aggregated.
- wal_file_timeout int: This is a timeout for one of the processes working on a WAL file. It is needed to prevent deadlock incase one of the processes cannot complete.
- tsc_file_optimization_timeout int: This is the timeout for a process to merge TSC files during the once a day tsc file optimization. The timeout is for one process to merge one measure device combination not the entire dataset. If you are running the optimizer on a dataset that has never been optimized and has lots of files you may have to increase this value temporarily. It also may take multiple rounds of optimization to finish since the code is limited to doing 100_000 blocks of a measure device combination at one time..
- optimal_block_num_values int: This specifies the optimal number of values to put into a single block. The higher this number is the smaller your block_index table will be. However, if you make it too big your read performance will suffer when asking for smaller segments of data since the sdk will have to decompress the entire block.
- tsc_optimizer_run_time int: This is the hour of the day (0h-24h) you want the tsc file optimizer to run, if you don't want it to run set this value to -1
- target_tsc_file_size int: This is how big you want your tsc files to be in bytes, bigger files means less files to open when reading which may improve speed (depending on your system)
- num_blocks_checksum int: This is the number of blocks to hash at one time, the bigger this is the faster the tsc file optimizer will run, but you will use more RAM memory.
- create_dataset bool: This specifies if you want the tsc generator to create a dataset at startup. It will not overwrite a dataset if one already exists. This is good for dev if you are constantly needing to restart.
- interval_index_mode str: Determines the mode for writing data to the interval index. Modes include "disable", "fast", and "merge". The default is "merge" and it is recommended to keep it this way. Unless you have really gappy data and it is slowing down the tsc generator too much. For more information on this setting see the write_data function in the Atriumdb docs.
- gap_tolerance int: This is the number of nanoseconds that you are willing to tolerate before two intervals are merged into one. If this is 0 any discontinuity in the frequency of the timestamps will create a new interval. This can quickly lead to a lot of intervals in the interval index and make your database size grow quickly (which can slow down queries). 
  The main idea of the intervals is so you can find areas of time in the dataset that have data. So really this tolerance should be set to what you consider continuous data. Generally that means the length of a patients stay in a bed, less any times they were disconnected from the monitor so 5000000000 nanoseconds (5 seconds) is what we made the default but depending on the monitor your collecting from this may or may not be necessary.
- metadb_connection str: This is the name of the metadata database connection and should match the one specified in the config.

## Meta Database
This is the backend database that contains all of the information put into AtriumDB. This is needed so the TSC generator can tell AtriumDB what information is stored in which TSC files. The config parameters to set here are:
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
- The TSC generator has to be networked to the meta database.
- The TSC generator CANNOT be used with SQLite
- The meta database should start up before the TSC generator since it has to connect to it
- This service was meant to be deployed in a docker-compose setup and an example compose file can be found in the repository