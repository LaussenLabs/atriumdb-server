# Introduction 
This is the code for the WAL writer. The WAL writer is the system that takes in the CMF (common message format) messages from 
RabbitMQ and aggregates them into WAL files based on common header information. After a set amount of time (usually 30 mins) 
the WAL Writer closes the WAL files so they can be picked up by the TSC generator and have their information aggregated and stored in AtriumDB.


# Getting Started
The WAL writer interacts with 3 or 4 systems depending on your setup. Those systems are the TSC generator, RabbitMQ, a database and optionally SiriDB. 
SiriDB is optional and only nessicary if you want real time data access from the API since there is a lag of about an hour from when data is first created 
till it is available in AtriumDB. Config parameters will have to be set in a yaml file called "config.yaml" for the WAL writer to work correctly and an 
example config can be found in the repository. An explination of those parameters can be found below catigorized by service.

- loglevel str: This sets the logging level. Can be one of ["debug", "info", "warning", "error", "critical"]
- dataset_location str: This sets the location of the wal folder that contains the WAL files and optionally the meta folder which contains the sqlite database file.
- timezone str: This sets the timezone you are in.
- instance_name str: This is the name that specifies this install for open telemetry metrics.

## WAL Writer
- wal_folder_path str: The path to the folder containing the WAL files.
- idle_timeout int: This is how long WAL files will stay open for in seconds. If the WAL writer sees a file thats been open for longer than this time it will close them so the TSC generator can pick them up.
- file_length_time int: This is how much data is written to a WAL file. For example 3600 would tell the WAL writer to write an hour of data to a WAL file.
- gc_schedule_min int: This is how often to look for idle files to close in minutes.
- enable_siri bool: This either enables or disables storing messages to SiriDB. SiriDB does slow down the ingest process slightly so not using this will increase message processing rates.
- inbound_queue str: Name of the RabbitMQ queue to receive messages from.
- prefetch_count int: Max number of unacknowledged messages to fetch from RabbitMQ at a time.
- metadb_connection str: This is the name of the metadata database connection and should match the one specified in the config.

## RabbitMQ
RabbitMQ's job is to route the CMF messages that come from upstream services containing the waveform or metric data to the WAL writer. 
It has several config parameters that need to be set: 
- encrypt bool: This parameter specifies if you want to encrypt the RabbitMQ connection or not. If this is true you will also have to specify the certificate_path vatriable.
- host str: The host name or IP address of the RabbitMQ server.
- port int: The port of the RabbitMQ server.
- username str: Username for RabbitMQ.
- password str: Password for RabbitMQ.
- certificate_path str: Only specify if encrypt is set to True. The path to the SSL certificate

## Meta Database
This is the backend database that contains all of the information put into AtriumDB. This is neesed so the WAL writer can input new devices and measures as they appear. The config parameters to set here are:
- type str: The type of database. Can be one of ["mysql", "sqlite", "mariadb"]
- host str: The host name or IP address of the database. This is not needed if the database is sqlite.
- port int: The port of the database. This is not needed if the database is sqlite.
- username str: Username for the database. This is not needed if the database is sqlite.
- password str: Password for the database. This is not needed if the database is sqlite.
- db_name str: Name of the database. This is not needed if the database is sqlite.

## SiriDB
- host str: The host name or IP address of the SiriDB server.
- port int: The port of the SiriDB server.
- admin_port int: This is the admin port of the SiriDB server and is used with the SiriDB admin tool to do admin tasks like creating or droping databases.
- username str: Username for SiriDB.
- password str: Password for SiriDB.
- db_name str: Name of the SiriDB database you want to store the data in.
- max_wait_retry int: When reconnecting to Siri wait 1,2,4,8...max_wait_retry seconds then continue trying to connect every max_wait_retry seconds
- connection_timeout int: If not connected to siri after the set amount of seconds throw a timeout error.
- data_expiration_time str: The amount of time to keep values in the database before deleting them. Can be set using a number followed by one of d, h, m or s.


# Docker
This service is deployed using Docker and there are several things to take into account when deploying this service.

## Volume Mapping
First you have to map several volumes. The two that are required are your host tsc folder to /data/tsc and your host wal folder to /data/wal. 
The wal folder is where your WAL files will sit and the tsc folder is where your TSC files  will sit. Both of these folders should be shared with the TSC generator. 
If you are using an sqlite database you will also have to map your hosts meta folder to /data/meta. If you chose to use encrypted RabbitMQ then you will also have to map
the folder where your certificate.pem file is to /certs. You also have to map the config.yaml file on your host machine to a file called config.yaml in the container.

## Other Considerations
- The WAL writer has to be networked to RabbitMQ and optionally SiriDB and the meta database if sqlite is not being used.
- RabbitMQ, SiriDB and the meta database should start up before the WAL writer since it has to connect to them.
- This service was meant to be deployed in a docker-compose setup and an example compose file can be found in the TSC generator repository.