# Databricks notebook source
# MAGIC %md #Export Table ACLs
# MAGIC 
# MAGIC Exports Table ACLS to a JSON file on DBFS, which can be imported using the Import_Table_ACLs script
# MAGIC 
# MAGIC Parameters:
# MAGIC - Databases: [Optional] comma separated list of databases to be exported, if empty all databases will be exported
# MAGIC - OutputPath: Path to write the exported file to 
# MAGIC 
# MAGIC Returns: (`dbutils.notebook.exit(exit_JSON_string)`)
# MAGIC - `{ "total_num_acls": <int>, "num_errors": <int> }` 
# MAGIC   - total_num_acls : valid ACL entries int the exported JSON
# MAGIC   - num_errors : error entries in the exported JSON, principal is set to `ERROR_!!!` and object_key and object_value are prefixed with `ERROR_!!!`
# MAGIC 
# MAGIC Execution: **Run the notebook on a cluster with Table ACL's enabled as a user who is an admin** 
# MAGIC 
# MAGIC Supported object types:
# MAGIC - Catalog: included if all databases are exported, not included if databases to be exported are specified
# MAGIC   - Database: included
# MAGIC     - Table: included
# MAGIC     - View: included (they are treated as tables with ObjectType `TABLE`)
# MAGIC - Anonymous Function: included (testing pending)
# MAGIC - Any File: included
# MAGIC 
# MAGIC Unsupported object types:
# MAGIC - User Function: Currently in Databricks SQL not supported - will add support later
# MAGIC 
# MAGIC JSON File format: Line of JSON objects, gzipped
# MAGIC 
# MAGIC - written as `.coalesce(1).format("JSON").option("compression","gzip")`
# MAGIC - each line contains a JSON object with the keys:
# MAGIC   - `Database`: string
# MAGIC   - `Principal`: string
# MAGIC   - `ActionTypes`: list of action strings: 
# MAGIC   - `ObjectType`: `(ANONYMOUS_FUNCTION|ANY_FILE|CATALOG$|DATABASE|TABLE|ERROR_!!!_<type>)` (view are treated as tables)
# MAGIC   - `ObjectKey`: string
# MAGIC   - `ExportTimestamp`: string
# MAGIC - error lines contains:
# MAGIC   - the special `Principal` `ERROR_!!!` 
# MAGIC   - `ActionTypes` contains one element: the error message, starting with `ERROR!!! :`
# MAGIC   - `Database`, `ObjectType`, `ObjectKey` are all prefixed with `ERROR_!!!_`
# MAGIC - error lines are ignored by the Import_Table_ACLs 
# MAGIC 
# MAGIC The JSON file is written as table, because on a cluster with Table ACLS activated, files cannot be written directly.
# MAGIC The output path will contain other files and diretories, starting with `_`, which can be ignored.
# MAGIC 
# MAGIC 
# MAGIC What to do if exported JSON contains errors (the notebook returns `num_errors` > 0):
# MAGIC - If there are only a few errors ( e.g. less then 1% or less then dozen)
# MAGIC   - proceed with the import (any error lines will be ignored)
# MAGIC   - For each error, the object type, name and error message is included so the cause can be investigated
# MAGIC     - in most cases, it turns out that those are broken or not used tables or views
# MAGIC - If there are many errors
# MAGIC   - Try executing some `SHOW GRANT` commands on the same cluster using the same user, there might be a underlying problem
# MAGIC   - review the errors and investiage

# COMMAND ----------

# DBTITLE 1,Declare Parameters
#dbutils.widgets.removeAll()
dbutils.widgets.text("Databases","db_acl_test,db_acl_test_restricted","1: Databases (opt)")
dbutils.widgets.text("OutputPath","dbfs:/tmp/migrate/test_table_acls.json.gz","2: Output Path")

# COMMAND ----------

# DBTITLE 1,Check Parameters
    
if not dbutils.widgets.get("OutputPath").startswith("dbfs:/"):
   raise Exception(f"Unexpected value for notebook parameter 'InputPath', got <{dbutils.widgets.get('OutputPath')}>, but it must start with <dbfs:/........>")


# COMMAND ----------

# DBTITLE 1,Params
WRITE_TABLES_PER_BATCH = 100
MAX_WORKERS = 8

# COMMAND ----------

# DBTITLE 1,Define Export Logic
import pyspark.sql.functions as sf
from typing import Callable, Iterator, Union, Optional, List
import datetime
import sys
from functools import reduce
from pyspark.sql import DataFrame

def create_error_grants_df(sys_exec_info_res, database_name: str,object_type: str, object_key: str):
  msg_context = f"context: database_name: {database_name}, object_type: {object_type}, object_key: {object_key}"

  msg_lines = str(sys_exec_info_res[1]).split("\n")
  if len(msg_lines) <= 2:
    short_message = " ".join(msg_lines)
  else:
    short_message = " ".join(msg_lines[:2])   
  error_message = f"ERROR!!! : exception class {sys_exec_info_res[0]},  message: {short_message}, {msg_context}".replace('"',"'")

  print(error_message)

  database_value = f"'ERROR_!!!_{database_name}'".replace('"',"'") if database_name else "NULL"
  object_key_value = f"'ERROR_!!!_{object_key}'".replace('"',"'") if object_key else "NULL"
  object_type_value = f"'ERROR_!!!_{object_type}'".replace('"',"'") if object_type else "NULL" # Import ignores this object type

  grants_df = spark.sql(f"""SELECT 
      {database_value} AS Database, 
      'ERROR_!!!' AS Principal,
      array("{error_message}") AS ActionTypes, 
      {object_type_value} AS ObjectType, 
      {object_key_value} AS ObjectKey, 
      Now() AS ExportTimestamp
   """)  

  return grants_df

def get_database_names():
  database_names = []
  for db in spark.sql("show databases").collect():
    if hasattr(db,"databaseName"): #Angela has this fallback ...
      database_names.append(db.databaseName)
    else:
      database_names.append(db.namespace)
  return database_names

def write_grants_df(df, output_path, writeMode="append"):
  try:
    (
      df
     .selectExpr("Database","Principal","ActionTypes","ObjectType","ObjectKey","ExportTimestamp")
     .sort("Database","Principal","ObjectType","ObjectKey")
     .write
     .format("delta")
     .mode(writeMode)
     .save(output_path)
    )
  except Exception as ex:
    print(ex)

def write_grants_dfs(dfs, output_path, writeMode="append"):
  union_df = reduce(DataFrame.unionAll, dfs)
  write_grants_df(union_df, output_path, writeMode)
  write_dfs = []  
  
def create_grants_df(database_name: str,object_type: str, object_key: str):
  try:
    if object_type in ["CATALOG", "ANY FILE", "ANONYMOUS FUNCTION"]: #without object key
       grants_df = (
         spark.sql(f"SHOW GRANT ON {object_type}")
         .groupBy("ObjectType","ObjectKey","Principal").agg(sf.collect_set("ActionType").alias("ActionTypes"))
         .selectExpr("CAST(NULL AS STRING) AS Database","Principal","ActionTypes","ObjectType","ObjectKey","Now() AS ExportTimestamp")
       )
    else: 
      grants_df = (
        spark.sql(f"SHOW GRANT ON {object_type} {object_key}")
        .filter(sf.col("ObjectType") == f"{object_type}")
        .groupBy("ObjectType","ObjectKey","Principal").agg(sf.collect_set("ActionType").alias("ActionTypes"))
        .selectExpr(f"'{database_name}' AS Database","Principal","ActionTypes","ObjectType","ObjectKey","Now() AS ExportTimestamp")
      )  
  except:  
    grants_df = create_error_grants_df(sys.exc_info(), database_name, object_type, object_key)
    
  return grants_df
  

def create_table_ACLSs_df_for_databases(database_names: List[str]):
  
  # TODO check Catalog heuristic:
  #  if all databases are exported, we include the Catalog grants as well
  #. if only a few databases are exported: we exclude the Catalog
#   if database_names is None or database_names == '':
#     database_names = get_database_names()
#     include_catalog = True
#   else:
#     include_catalog = False
    
  num_databases_processed = len(database_names)
  num_tables_or_views_processed = 0

#   # ANONYMOUS FUNCTION
#   combined_grant_dfs = create_grants_df(None, "ANONYMOUS FUNCTION", None)
  
#   # ANY FILE
#   combined_grant_dfs = combined_grant_dfs.unionAll(
#     create_grants_df(None, "ANY FILE", None)
#   )
  
#   # CATALOG
#   if include_catalog:
#     combined_grant_dfs = combined_grant_dfs.unionAll(
#       create_grants_df(None, "CATALOG", None)
#     )
  #TODO ELSE: consider pushing catalog grants down to DB level in this case
  for database_name in database_names:
    print(f"processing database {database_name}")
    # DATABASE
    grant_df = create_grants_df(database_name, "DATABASE", database_name)
    print("writing out grant_df")
    write_grants_df(grant_df, output_path)
    print("finish writing")
    try:
      print("getting tables")
      tables_and_views_rows = spark.sql(
        f"SHOW TABLES IN {database_name}"
      ).filter(sf.col("isTemporary") == False).collect()

      print(f"{datetime.datetime.now()} working on database {database_name} with {len(tables_and_views_rows)} tables and views")
      num_tables_or_views_processed = num_tables_or_views_processed + len(tables_and_views_rows)
     
      write_dfs = []
      for table_row in tables_and_views_rows:
        # TABLE, VIEW
        write_dfs.append(create_grants_df(database_name, "TABLE", f"{table_row.database}.{table_row.tableName}"))
        if len(write_dfs) == WRITE_TABLES_PER_BATCH:
          print("writing out write_dfs")
          write_grants_dfs(write_dfs, output_path)
          write_dfs = []
      if write_dfs:
        print("Flush out what's left in write_dfs.")
        write_grants_dfs(write_dfs, output_path)
        write_dfs = []
        
    except:  
      # error in SHOW TABLES IN database_name, errors in create_grants_df have already been catched
      error_df = create_error_grants_df(sys.exc_info(), database_name ,"DATABASE", database_name)
      write_grants_df(error_df, output_path)
     
    #TODO ADD USER FUNCTION - not supported in SQL Analytics, so this can wait a bit
    # ... SHOW USER FUNCTIONS LIKE  <my db>.`*`;
    #. function_row['function']   ... nah does not seem to work
          
  return num_databases_processed, num_tables_or_views_processed


# COMMAND ----------

output_path = dbutils.widgets.get("OutputPath")

# COMMAND ----------

# DBTITLE 1,Run Export
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import os, fnmatch, shutil

databases_raw = dbutils.widgets.get("Databases")

def process_db(db: str, writeMode: str):
  print(f"processing db {db} with mode {writeMode}")
  num_databases_processed, num_tables_or_views_processed = create_table_ACLSs_df_for_databases([db])
  print(f"{datetime.datetime.now()} total number processed: databases: {num_databases_processed}, tables or views: {num_tables_or_views_processed}")


def append_db_acls(db: str):
  process_db(db, "append")  
  
if databases_raw.rstrip() == '':
  databases = get_database_names()
  include_catalog = True
  print(f"Exporting all databases")
else:
  databases = [x.rstrip().lstrip() for x in databases_raw.split(",")]
  include_catalog = False
  print(f"Exporting the following databases: {databases}")



# ANONYMOUS FUNCTION
combined_grant_dfs = create_grants_df(None, "ANONYMOUS FUNCTION", None)
  
# ANY FILE
combined_grant_dfs = combined_grant_dfs.unionAll(
  create_grants_df(None, "ANY FILE", None)
)
  
# CATALOG
if include_catalog:
  combined_grant_dfs = combined_grant_dfs.unionAll(
    create_grants_df(None, "CATALOG", None)
  )  

write_grants_df(combined_grant_dfs, output_path, "overwrite")

with ThreadPoolExecutor(max_workers = MAX_WORKERS) as executor:
  results = executor.map(append_db_acls, databases)
print(results)

# print(f"{datetime.datetime.now()} total number processed: databases: {total_databases_processed}, tables or views: {total_tables_processed}")
print(f"{datetime.datetime.now()} writing table ACLs to {output_path}")

# COMMAND ----------

# DBTITLE 1,Optimize the output data
spark.sql(f"optimize delta.`{output_path}`")

# COMMAND ----------

# DBTITLE 1,Exported Table ACLs
display(spark.read.format("delta").load(output_path)) 

# COMMAND ----------

# DBTITLE 1,Write total_num_acls and num_errors to the notebook exit value
totals_df = (spark.read
      .format("delta")
      .load(output_path)
      .selectExpr(
         "sum(1) AS total_num_acls"
        ,"sum(CASE WHEN Principal = 'ERROR_!!!' THEN 1 ELSE 0 END) AS num_errors")
     )

res_rows = totals_df.collect()

exit_JSON_string = '{ "total_num_acls": '+str(res_rows[0]["total_num_acls"])+', "num_errors": '+str(res_rows[0]["num_errors"])+' }'

print(exit_JSON_string)

dbutils.notebook.exit(exit_JSON_string) 

# COMMAND ----------


