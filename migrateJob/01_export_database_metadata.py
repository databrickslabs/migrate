# Databricks notebook source
db_name = dbutils.widgets.get("db_name")

# COMMAND ----------

import json

from py4j.protocol import Py4JJavaError

from pyspark.sql import Catalog
from pyspark.sql.functions import col
from pyspark.sql.utils import AnalysisException

import pandas as pd


def check_df(df, key, value):
    """ Checks properties of a table in their `describe table extended` data.
    
    Args:
        df (DataFrame):  dataframe containing the results of spark.sql("describe table extended db_name.table_name")
        key (Str): Property in col_name to check (eg. "Type", "Provider")
        value (Str): Property value in data_type to validate against (eg. "MANAGED", "delta")

    returns (Bool): evaluation of value check
    """
    try:
        validate_this = df.filter(col("col_name") == key).select(col("data_type")).collect()[0][0]
        return validate_this == value
    except IndexError:
        return False

# outputs
all_tables = list()
delta_json = list()
parquet_json = list()
logs = list()
base_path = "/dbfs/FileStore/hive-migration-metadata/"
delta_json_loc = base_path + "delta.json"
parquet_json_loc = base_path + "parquet.json"
all_tables_loc = base_path + "all_tables.json"
logs_loc = base_path + "errors.log"

catalog = Catalog(spark)
if db_name:
    db_list = (db_name,)
else:
    # get database names
    db_list = [x.name for x in catalog.listDatabases()]

# loop through database names
for db in db_list:
    table_list = [x.name for x in catalog.listTables(db)]
    # loop through table names
    for table in table_list:
        try:
            df = spark.sql(f"describe table extended {db}.{table}")
        except (Py4JJavaError, AnalysisException) as e:
            logs.append({"db_name": db, "table_name": table, "error_message": str(e)})
            continue
            
        is_managed = check_df(df, "Type", "MANAGED")
        is_delta = check_df(df, "Provider", "delta") # views don't have provider col_name entry
        is_parquet = check_df(df, "Provider", "parquet")   
        is_partitioned = check_df(df, "# col_name", "data_type") # un-partitioned tables don't have this property
        
        table_data = {"db_name" : db, "table_name" : table}
        all_tables.append(table_data)
        if is_managed & is_delta:
            delta_json.append(table_data)
        elif is_parquet & is_partitioned:
            parquet_json.append(table_data)

# COMMAND ----------

import os

# remove files if they exist
if os.path.exists(delta_json_loc):
    os.remove(delta_json_loc)
if os.path.exists(parquet_json_loc):
    os.remove(parquet_json_loc)
if os.path.exists(logs_loc):
    os.remove(logs_loc)
if os.path.exists(all_tables_loc):
    os.remove(all_tables_loc)

    
if not os.path.exists(base_path):
    os.makedirs(base_path)
    
# save delta tables to dbfs
with open(delta_json_loc, "w") as outfile:
    json.dump(delta_json, outfile)

# save parquet tables to dbfs
with open(parquet_json_loc, "w") as outfile:
    json.dump(parquet_json, outfile)

# save errors to dbfs
with open(logs_loc, "w") as outfile:
    for error in logs:
        json.dump(error, outfile)

# save all tables to dbfs
with open(all_tables_loc, "w") as outfile:
    for table in all_tables:
        json.dump(table, outfile)

# COMMAND ----------

import json
with open(delta_json_loc) as f:
    df_json = json.load(f)
print(df_json)

# COMMAND ----------

import json
with open(parquet_json_loc) as f:
    df_json = json.load(f)
print(df_json)

# COMMAND ----------

# import json
# with open(all_tables_loc) as f:
#     df_json = json.load(f)
# print(df_json)