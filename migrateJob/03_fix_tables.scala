// Databricks notebook source
// DBTITLE 1,Repair delta tables
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.functions.{udf, col}
import org.apache.spark.sql.types.{StructType, StringType}

def _update_table_properties (db_name : String, table_name : String): Int = {
  try {
    val identifier = TableIdentifier(table_name, Some(db_name))
    val oldTable = spark.sessionState.catalog.getTableMetadata(identifier)
    val newTableType = CatalogTableType.MANAGED
    val alteredTable = oldTable.copy(tableType = newTableType)
    spark.sessionState.catalog.alterTable(alteredTable) 
    return 0
  } catch {
    case _: Throwable => 1
  }
}
// register helper function as spark udf
val update_table_properties = spark.udf.register("update_table_properties", _update_table_properties _)

// define schema
val schema = new StructType()
  .add("db_name", StringType, false)
  .add("table_name", StringType, false)

// read in data for which dbs need to be changed back
var df = spark.read.schema(schema).json("dbfs:/FileStore/hive-migration-metadata/delta.json")

// COMMAND ----------

// apply udf to loaded 
// var df_result = df.withColumn("result", update_table_properties(df("db_name"), df("table_name")))

// display failed migrations
// display(df_result.filter(col("result") === 1))

// UDF not executing, temporary workaround
for (row <- df.rdd.collect)
{   
    var db_name = row.mkString(",").split(",")(0)
    var table_name = row.mkString(",").split(",")(1)
    var res = _update_table_properties(db_name, table_name)
    println("")
    if (res == 0) {
      println(s"successful update $db_name.$table_name")
    } else {
      println(s"ERROR unsuccessful update $db_name.$table_name")
    }
}

// COMMAND ----------

// DBTITLE 1,Repair parquet tables
// MAGIC %python
// MAGIC from pyspark.sql.types import StructType, StringType
// MAGIC from pyspark.sql.utils import AnalysisException
// MAGIC 
// MAGIC 
// MAGIC parquet_json_loc = "/FileStore/hive-migration-metadata/parquet.json"
// MAGIC 
// MAGIC schema = StructType()\
// MAGIC   .add("db_name", StringType(), False)\
// MAGIC   .add("table_name", StringType(), False)
// MAGIC df = spark.read.schema(schema).json(parquet_json_loc)
// MAGIC 
// MAGIC for row in df.collect():
// MAGIC     try:
// MAGIC         spark.sql(f"msck repair table {row.db_name}.{row.table_name}")
// MAGIC         print(f"repaired table {row.db_name}.{row.table_name}")
// MAGIC     except AnalysisException as e:
// MAGIC         print(f"failed to repair table: {row.db_name}.{row.table_name}")
// MAGIC         print(e)
// MAGIC         continue