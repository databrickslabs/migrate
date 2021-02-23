# Databricks notebook source
migration_log = '/dbfs/tmp/migration/repair_ddl.log'

num_of_tables = 0
with open(migration_log, 'r') as fp:
    for line in fp:
        # this is the db_name.tbl_name value
        fqdn_table = line.rstrip()
        fix_sql_statement = f"MSCK REPAIR TABLE {fqdn_table}"
        print(fix_sql_statement)
        df = spark.sql(fix_sql_statement)
        num_of_tables += 1

# COMMAND ----------

print(f"Total number of tables repaired {num_of_tables}")

# COMMAND ----------


