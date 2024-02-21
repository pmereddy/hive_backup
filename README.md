# hive_backup

## Description
The hive_backup script facilitates the creation of a schema backup for a Hive database using the Hive metastore. Currently, it supports hive metastores using PostgreSQL database only.

## Motivation
Taking a schema backup of tables within a Hive database using "show create table" with Beeline, can be time-consuming, especially for databases with thousands of tables. A large backup window could potentially lead to more inconsistencies in the backup. Batching of multiple "show create table" statements is incrementally better. But, the backup still takes many hours with 1000's of hive tables. 
This script takes a direct approach, connects to the metastore and reconstructs the DDL using the metadata stored in metastore tables. This approach is 100x-500x faster than using beeline or impala shell.


## Limitations
1. `RDBMS Dependency`: The script is tightly coupled with the type of relational database management system (RDBMS) used for the Hive metastore. As of this release, only PostgreSQL is supported.
2. `Metastore Schema Changes`: Any schema changes within the metastore, especially during major releases, could potentially break the functionality of this script. It's important to monitor changes in the metastore schema and adapt the script accordingly.
3. `SKEWED columns`: The script does not currently support recreating the skewed columns segment of the Data Definition Language (DDL) for backed-up tables. This feature is planned for a future release.

## How to run it
To create a hive database backup for "apodev" and save it to apodev_xxxxxx.hql, run the following command
```
python3 ./hive_database_ddl_backup_using_metastore.py --database apodev  --password '<password>' --host <hive_metastore_host> --port <dbport> --filename apodev_xxxxxx.hql --log_level 'DEBUG'
```

## Example output

```
2024-02-21 16:25:41,685 - INFO - Extracting DDL for database: apodev
2024-02-21 16:25:41,687 - INFO - Found 40 tables in apodev
2024-02-21 16:25:41,747 - INFO - DDL saved to apodev_xxxxxx.hql successfully.

```
