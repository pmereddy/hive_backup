# hive_backup

## Description
The script creates a schema backup of a hive database using metastore. 
In this first release, it supports postgresql database only.

## Motivation
Taking a schema backup of tables in a hive database using beeline takes an enormous amount of time. Batching of multiple "show create table" statements is incrementally better. 
But, the backup still takes many hours with 1000's of hive tables. A large backup window could potentially lead to more inconsistencies in the backup.

## Limitations
1. Is tightly coupled with the type of RDBMS used for metastore. In this release, only postgresql is supported
2. Schema changes in metastore (could happen during major releases) could potentially break this script.
3. The SKEWED COLUMNS segment of the DDL is not recreated. But, will be addressed soon.

## How to run it
To create a hive database backup for "apodev" and save it to apodev_xxxxxx.hql, run the following command
```
python3 ./hive_database_ddl_backup_using_metastore.py --database apodev  --password '<password>' --host <hive_metastore_host> --port <dbport> --filename apodev_xxxxxx.hql --log_level 'DEBUG'```
## Example output
```
2024-02-21 16:25:41,685 - INFO - Extracting DDL for database: apodev
2024-02-21 16:25:41,687 - INFO - Found 40 tables in apodev
2024-02-21 16:25:41,747 - INFO - DDL saved to apodev_xxxxxx.hql successfully.
```
