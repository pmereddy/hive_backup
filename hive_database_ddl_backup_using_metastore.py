"""
Script Name: hive_database_ddl_backup_using_metastore.py
Description: This script connects to the hive metastore database (postgresql) directly, extracts metadata 
        for each table in a given hive database, creates DDL statement and saves it to the outputfile.
Version: 1
Author: Pramodh Mereddy

Disclaimer: This script is fairly new and has not been tested very well in the field. Please verify before using it.
"""

import logging
import argparse
import psycopg2
from psycopg2 import pool


table_ddl_queries = {
    'Q1' : 'SELECT DISTINCT "A0"."CREATE_TIME","A0"."TBL_ID","A0"."LAST_ACCESS_TIME","A0"."OWNER","A0"."OWNER_TYPE","A0"."RETENTION","A0"."IS_REWRITE_ENABLED","A0"."TBL_NAME","A0"."TBL_TYPE","A0"."WRITE_ID" FROM "TBLS" "A0" LEFT OUTER JOIN "DBS" "B0" ON "A0"."DB_ID" = "B0"."DB_ID" WHERE "A0"."TBL_NAME" = \'{table}\' AND "B0"."NAME" = \'{catalog}\' AND "B0"."CTLG_NAME" = \'{database}\'',
    'Q2' : 'SELECT "B0"."CTLG_NAME","B0"."DATACONNECTOR_NAME","B0"."CREATE_TIME","B0"."DESC","B0"."DB_LOCATION_URI","B0"."DB_MANAGED_LOCATION_URI","B0"."NAME","B0"."OWNER_NAME","B0"."OWNER_TYPE","B0"."REMOTE_DBNAME","B0"."TYPE","B0"."DB_ID","C0"."INPUT_FORMAT","C0"."IS_COMPRESSED","C0"."IS_STOREDASSUBDIRECTORIES","C0"."LOCATION","C0"."NUM_BUCKETS","C0"."OUTPUT_FORMAT","C0"."SD_ID","A0"."VIEW_EXPANDED_TEXT","A0"."VIEW_ORIGINAL_TEXT" FROM "TBLS" "A0" LEFT OUTER JOIN "DBS" "B0" ON "A0"."DB_ID" = "B0"."DB_ID" LEFT OUTER JOIN "SDS" "C0" ON "A0"."SD_ID" = "C0"."SD_ID" WHERE "A0"."TBL_ID" = {table_id}',
    'Q3' : 'SELECT "A0"."PARAM_KEY","A0"."PARAM_VALUE" FROM "TABLE_PARAMS" "A0" WHERE "A0"."TBL_ID" = {table_id} AND "A0"."PARAM_KEY" IS NOT NULL',
    'Q4' : 'SELECT "A0"."PKEY_COMMENT","A0"."PKEY_NAME","A0"."PKEY_TYPE","A0"."INTEGER_IDX" AS "NUCORDER0" FROM "PARTITION_KEYS" "A0" WHERE "A0"."TBL_ID" = {table_id} AND "A0"."INTEGER_IDX" >= 0 ORDER BY "NUCORDER0"',
    'Q5' : 'SELECT "B0"."CD_ID" FROM "SDS" "A0" LEFT OUTER JOIN "CDS" "B0" ON "A0"."CD_ID" = "B0"."CD_ID" WHERE "A0"."SD_ID" = {sd_id}',
    'Q6' : 'SELECT "A0"."COLUMN_NAME","A0"."ORDER","A0"."INTEGER_IDX" AS "NUCORDER0" FROM "SORT_COLS" "A0" WHERE "A0"."SD_ID" = {sd_id} AND "A0"."INTEGER_IDX" >= 0 ORDER BY "NUCORDER0"',
    'Q7' : 'SELECT "A0"."BUCKET_COL_NAME","A0"."INTEGER_IDX" AS "NUCORDER0" FROM "BUCKETING_COLS" "A0" WHERE "A0"."SD_ID" = {sd_id} AND "A0"."INTEGER_IDX" >= 0 ORDER BY "NUCORDER0"',
    'Q8' : 'SELECT "A0"."PARAM_KEY","A0"."PARAM_VALUE" FROM "SD_PARAMS" "A0" WHERE "A0"."SD_ID" = {sd_id}  AND "A0"."PARAM_KEY" IS NOT NULL',
    'Q9' : 'SELECT "A0"."COMMENT","A0"."COLUMN_NAME","A0"."TYPE_NAME","A0"."INTEGER_IDX" AS "NUCORDER0" FROM "COLUMNS_V2" "A0" WHERE "A0"."CD_ID" = {table_id} AND "A0"."INTEGER_IDX" >= 0 ORDER BY "NUCORDER0"',
    'Q10': 'SELECT "B0"."DESCRIPTION","B0"."DESERIALIZER_CLASS","B0"."NAME","B0"."SERDE_TYPE","B0"."SLIB","B0"."SERIALIZER_CLASS","B0"."SERDE_ID" FROM "SDS" "A0" LEFT OUTER JOIN "SERDES" "B0" ON "A0"."SERDE_ID" = "B0"."SERDE_ID" WHERE "A0"."SD_ID" = {sd_id}',
    'Q11': 'SELECT "A0"."PARAM_KEY","A0"."PARAM_VALUE" FROM "SERDE_PARAMS" "A0" WHERE "A0"."SERDE_ID" = {serde_id} AND "A0"."PARAM_KEY" IS NOT NULL',
    'Q12': 'SELECT "A0"."SKEWED_COL_NAME","A0"."INTEGER_IDX" AS "NUCORDER0" FROM "SKEWED_COL_NAMES" "A0" WHERE "A0"."SD_ID" = {sd_id} AND "A0"."INTEGER_IDX" >= 0 ORDER BY "NUCORDER0"',
    'Q13': 'SELECT \'org.apache.hadoop.hive.metastore.model.MStringList\' AS "DN_TYPE","A1"."STRING_LIST_ID","A0"."INTEGER_IDX" AS "NUCORDER0" FROM "SKEWED_VALUES" "A0" INNER JOIN "SKEWED_STRING_LIST" "A1" ON "A0"."STRING_LIST_ID_EID" = "A1"."STRING_LIST_ID" WHERE "A0"."SD_ID_OID" = {serde_id} AND "A0"."INTEGER_IDX" >= 0 ORDER BY "NUCORDER0"',
    'Q14': 'SELECT \'org.apache.hadoop.hive.metastore.model.MStringList\' AS "DN_TYPE","A0"."STRING_LIST_ID" FROM "SKEWED_STRING_LIST" "A0" INNER JOIN "SKEWED_COL_VALUE_LOC_MAP" "B0" ON "A0"."STRING_LIST_ID" = "B0"."STRING_LIST_ID_KID" WHERE "B0"."SD_ID" = {sd_id}',
    'Q15': 'SELECT "A0"."STRING_LIST_ID_KID","A0"."LOCATION" FROM "SKEWED_COL_VALUE_LOC_MAP" "A0" WHERE "A0"."SD_ID" = {sd_id} AND NOT ("A0"."STRING_LIST_ID_KID" IS NULL)',
    'Q16': 'SELECT "A0"."PKEY_COMMENT","A0"."PKEY_NAME","A0"."PKEY_TYPE","A0"."INTEGER_IDX" AS "NUCORDER0" FROM "PARTITION_KEYS" "A0" WHERE "A0"."TBL_ID" = {table_id} AND "A0"."INTEGER_IDX" >= 0 ORDER BY "NUCORDER0"',
    'Q17': 'SELECT "PARAM_KEY", "PARAM_VALUE" FROM "SDS" JOIN "SD_PARAMS" ON "SDS"."CD_ID" = "SD_PARAMS"."SD_ID" WHERE "SDS"."SD_ID" = {sd_id} and "PARAM_KEY"=\'buclet_cols\''
}

def backup_database_ddl(database, catalog, output_file):
    try:
        logging.info(f"Extracting DDL for database: {catalog}")
        connection = connection_pool.getconn()
        cursor = connection.cursor()
        table_list_cmd = f'select "TBL_NAME" from "TBLS" where "TBL_TYPE" not in (\'VIRTUAL_VIEW\',\'MATERIALIZED_VIEW\') and "DB_ID"=(select "DB_ID" from "DBS" where "NAME"=\'{catalog}\') order by "TBL_ID"'
        cursor.execute(table_list_cmd)
        rows = cursor.fetchall()
        logging.info(f"Found {len(rows)} tables in {catalog}")
        with open(output_file, 'w') as fd:
            for row in rows:
                backup_table_ddl(database, catalog, row[0], table_ddl_queries, fd)
        logging.info(f"DDL saved to {output_file} successfully.")
    except Exception as e:
        logging.error(f"An error occurred in backup_database_ddl: {str(e)}")
        connection.rollback()
    finally:
        cursor.close()
        connection_pool.putconn(connection)

def backup_table_ddl(database, catalog, table, table_ddl_queries, ofd):
    connection = connection_pool.getconn()
    cursor = connection.cursor()
    create_statement=""
    try:
        table_id=0
        serde_id=0
        sd_id=0
        column_string=""
        tbl_properties=""
        serde_properties=""
        stored_by=""
        location_string=""
        row_format=""
        format_string=""
        partition_key_string=""
        table_comment=""
        sorted_by_string=""
        for query_name, query_template in table_ddl_queries.items():
            formatted_query = query_template.format(database=database, catalog=catalog, table=table, table_id=table_id, serde_id=serde_id, sd_id=sd_id)
            logging.debug(f"Executing query {query_name} : {formatted_query}")
            cursor.execute(formatted_query)
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            results = [dict(zip(cols, row)) for row in rows]
            logging.debug(f"Results for {query_name} : {results}")
            if query_name == 'Q1':
                if len(results) > 0:
                    table_name = results[0].get('TBL_NAME')
                    table_type = results[0].get('TBL_TYPE')
                    table_id = results[0].get('TBL_ID')
                else:
                    logging.warning("Table: {table} not found in the metastore")
                    return
            if query_name == 'Q2':
                if len(results) > 0:
                    sd_id = results[0].get('SD_ID')
                    location = results[0].get('LOCATION')
                    db_location_uri = results[0].get('DB_LOCATION_URI')
                    num_buckets = results[0].get('NUM_BUCKETS')
                    if num_buckets > 0:
                        bucket_string=f"\nINTO {num_buckets} BUCKETS"
                    else:
                        bucket_string=f""
                    if db_location_uri and location:
                        if not db_location_uri in location:
                            location_string=f"\nLOCATION\n  '{location}'"
                    ipf = results[0].get('INPUT_FORMAT')
                    opf = results[0].get('OUTPUT_FORMAT')
                    if ipf is not None or opf is not None:
                        if ipf is not None:
                            format_string=f"\nSTORED AS INPUTFORMAT\n  '{ipf}'"
                        if opf is not None:
                            format_string += f"\nOUTPUTFORMAT\n  '{opf}'"
            if query_name == 'Q3':
                arr = []
                for entry in results:
                    if entry['PARAM_KEY'] == 'COLUMN_STATS_ACCURATE' or entry['PARAM_KEY'] == 'EXTERNAL':
                        continue
                    if entry['PARAM_KEY'] == 'storage_handler':
                        stored_by=f"\nSTORED BY\n  '{entry['PARAM_VALUE']}'"
                        continue
                    if entry['PARAM_KEY'] == 'comment':
                        table_comment=f"\nCOMMENT '{entry['PARAM_VALUE']}'"
                        continue
                    if entry['PARAM_KEY'] == 'numFiles' or entry['PARAM_KEY'] == 'numFilesErasureCoded' or entry['PARAM_KEY'] == 'totalSize':
                        continue
                    arr.append(f"'{entry['PARAM_KEY']}'='{entry['PARAM_VALUE']}'")
                if len(arr) > 0:
                    a=',\n  '.join(arr)
                    tbl_properties=f"\nTBLPROPERTIES(\n  {a})"
                else:
                    tbl_properties=""
            if query_name == 'Q6':
                arr = []
                for entry in results:
                    arr.append(f"{entry['COLUMN_NAME']} {entry['ORDER']}")
                if len(arr) > 0:
                    a=', '.join(arr)
                    sorted_by_string=f" SORTED BY ({a})"
            if query_name == 'Q9':
                arr = []
                for entry in results:
                  if entry['COMMENT'] is None:
                    arr.append(f"`{entry['COLUMN_NAME']}` {entry['TYPE_NAME']}")
                  else:
                    arr.append(f"`{entry['COLUMN_NAME']}` {entry['TYPE_NAME']} COMMENT '{entry['COMMENT']}'")
                if len(arr) > 0:
                    column_string=',\n  '.join(arr)

            if query_name == 'Q10':
                if len(results) > 0:
                    deser = results[0].get('DESERIALIZER_CLASS')
                    ser = results[0].get('SERIALIZER_CLASS')
                    slib = results[0].get('SLIB')
                    serde_id = results[0].get('SERDE_ID')
                    row_format=f"\nROW FORMAT SERDE\n  '{slib}'"

            if query_name == 'Q11':
                arr = []
                for entry in results:
                    arr.append(f"'{entry['PARAM_KEY']}'='{entry['PARAM_VALUE']}'")
                if len(arr) > 0:
                    a=',\n '.join(arr)
                    serde_properties=f"\nWITH SERDEPROPERTIES (\n  {a}\n)"

            if query_name == 'Q16':
                arr = []
                for entry in results:
                    arr.append(f"`{entry['PKEY_NAME']}` {entry['PKEY_TYPE']}")
                if len(arr) > 0:
                    partition_key_string="\nPARTITIONED BY (\n  "+', '.join(arr)+")"

            if query_name == 'Q17':
                clustered_by_string=""
                if len(results) > 0:
                    bucket_cols = results[0].get('PARAM_VALUE')
                    if bucket_cols is not None:
                        clustered_by_string=f"\nCLUSTERED BY ({bucket_cols}) {sorted_by_string} {bucket_string}"

        create_statement = f"CREATE {table_type} `{catalog}`.`{table_name}`(\n  {column_string}) {table_comment} {partition_key_string} {clustered_by_string} {row_format} {format_string} {location_string} {stored_by} {serde_properties} {tbl_properties};\n\n"
        ofd.write(create_statement)
        connection.commit()
    except Exception as e:
        connection.rollback()
        print("Error:", e)
    finally:
        cursor.close()
        connection_pool.putconn(connection)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Arguments to the backup script.")
    parser.add_argument("--hms_database", type=str, help="Name of the hive metastore database", default='hive')
    parser.add_argument("--log_level", type=str, help="log level", choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], default='INFO')
    parser.add_argument("--database", type=str, help="Name of the hive database", required=True)
    parser.add_argument("--user", type=str, help="Name of the database user", default='hive')
    parser.add_argument("--host", type=str, help="Name of the HMS database host", default='localhost')
    parser.add_argument("--password", type=str, help="password for the database user", required=True)
    parser.add_argument("--port", type=int, help="port the database is running on", default=5432)
    parser.add_argument("--filename", type=str, help="Output filename", default='ddl_backup.hql')

    args = parser.parse_args()
    hms_database = args.hms_database
    log_level = args.log_level
    catalog = args.database
    user = args.user
    host = args.host
    password = args.password
    port = args.port
    output_file=args.filename
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s')

    connection_pool = psycopg2.pool.SimpleConnectionPool(
			minconn=1, maxconn=3,
                        dbname=hms_database,
                        user=user,
                        password=password,
                        host=host,
                        port=port)

    backup_database_ddl(hms_database, catalog, output_file)
