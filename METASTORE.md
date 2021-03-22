# Databricks Metastore Migration

This document discusses the metastore migration options and process. 

1. Export the metastore DDL 
2. Import the metastore DDL  
   a. The tool will import `TABLES` first  
   b. The tool will sideline `VIEWS` to be applied after all tables are created. Views will be sidelined into 
      `metastore_views/` directory in the export directory.   
   c. The tool will import all `VIEWS`   
3. Copy the underlying DBFS / root table data. Databricks support team will need to help with this step.  
4. Report on legacy table DDLs to be repaired within the new workspace and metastore.   
   a. Use the `--get-repair-log` option with the import tool. This will generate a list of tables that need to be 
   repaired. The most common case of this is to register hive partitions within the metastore.  
   b. The repair option will upload a list of tables to be repaired, and users can use the notebook included in this 
   repo, `data/repair_tables_for_migration.py`, to run this operation. 


**Recommendation / Caveats:**
1. Use the `--metastore-unicode` option to export and import if you do not know if tables contain unicode characters. 
   This should be applied to both export and import operations.
2. Use DBR 6.x / Spark 2.x releases if you have legacy table definitions. 
   Spark 3.x deprecates `SERDE` support and can cause import issues if you require those tables to use `SERDE` 
   definitions. 
3. If you manually register table partitions using `ALTER TABLE table_name ADD PARTITION ()` to tables, you will need 
   to manually report and add these partitions. The tool does not support this today. 
   Or if you need to drop partitions, you can use `ALTER TABLE table_name DROP PARTITION ()`