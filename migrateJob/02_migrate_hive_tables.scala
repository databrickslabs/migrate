// Databricks notebook source
// MAGIC %python
// MAGIC import os 
// MAGIC 
// MAGIC # dbutils.widgets.text('lower_cluster', 'cluster1.2.1')
// MAGIC # dbutils.widgets.text('higher_cluster', 'cluster3.1.0')
// MAGIC # dbutils.widgets.text('db_name', '')
// MAGIC 
// MAGIC lower_cluster = dbutils.widgets.get('lower_cluster')
// MAGIC higher_cluster = dbutils.widgets.get('higher_cluster')
// MAGIC 
// MAGIC if not isinstance(lower_cluster, (int, str)) or not isinstance(higher_cluster, (int, str)):
// MAGIC   raise ValueError('please provide a valid cluster name')
// MAGIC else:
// MAGIC   os.environ["LOWER_CLUSTER"] = lower_cluster
// MAGIC   os.environ["HIGHER_CLUSTER"] = higher_cluster
// MAGIC 
// MAGIC db_name = dbutils.widgets.get('db_name')
// MAGIC if db_name == "":
// MAGIC   os.environ["JUST_ONE_DB"] = "false"
// MAGIC else:
// MAGIC   os.environ["JUST_ONE_DB"] = "true"
// MAGIC   os.environ["DB_NAME"] = db_name

// COMMAND ----------

// DBTITLE 1,Create a Databricks client profile.
val host = dbutils.secrets.get("migration-params", "host")
val token = dbutils.secrets.get("migration-params", "token")

val defaultProfile = s"""[DEFAULT]
host=$host
token=$token
"""
dbutils.fs.put("file:///root/.databrickscfg", defaultProfile, true)

// COMMAND ----------

// DBTITLE 1,Install the migration tool on the driver.
// MAGIC %sh 
// MAGIC mkdir /tmp/hive-upgrade
// MAGIC cd /tmp/hive-upgrade
// MAGIC git clone -b bp-features --single-branch https://github.com/attilaszuts/migrate.git
// MAGIC cd /tmp/hive-upgrade/migrate
// MAGIC python3 setup.py install
// MAGIC pip install requests==2.27.1 -U
// MAGIC pip install urllib3==1.26.9 -U

// COMMAND ----------

// DBTITLE 1,Clean directory
// MAGIC %scala
// MAGIC dbutils.fs.rm("file:///tmp/hive-upgrade/migration-data", true)
// MAGIC // dbutils.fs.rm("dbfs:/tmp/hive-upgrade/migration-data", true)

// COMMAND ----------

// DBTITLE 1,Extract metadata
// MAGIC %sh
// MAGIC cd /tmp/hive-upgrade/migrate
// MAGIC if [ $JUST_ONE_DB = true ]
// MAGIC then
// MAGIC   python3 export_db.py --profile DEFAULT --metastore --cluster-name $LOWER_CLUSTER --azure --set-export-dir "/tmp/hive-upgrade/migration-data/metadata" --database $DB_NAME
// MAGIC else
// MAGIC   python3 export_db.py --profile DEFAULT --metastore --cluster-name $LOWER_CLUSTER --azure --set-export-dir "/tmp/hive-upgrade/migration-data/metadata"
// MAGIC fi 

// COMMAND ----------

// DBTITLE 1,Import metadata
// MAGIC %sh
// MAGIC cd /tmp/hive-upgrade/migrate
// MAGIC python3 import_db.py --profile DEFAULT --metastore --cluster-name $HIGHER_CLUSTER --azure --set-export-dir "/tmp/hive-upgrade/migration-data/metadata"

// COMMAND ----------

// MAGIC %sh 
// MAGIC cat /tmp/hive-upgrade/migration-data/metadata/database_details.log

// COMMAND ----------

// MAGIC %sh
// MAGIC 
// MAGIC ls /tmp/hive-upgrade/migration-data/metadata/metastore/sampledb