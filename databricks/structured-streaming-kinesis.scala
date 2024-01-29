// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %sql
// MAGIC -- create or replace table 

// COMMAND ----------

// Read Kinesis stream
val sdf = (
    spark.readStream.format("kinesis")
    .option("streamName", "demo-raw-events")
    .option("initialPosition", "trim_horizon")
    .option("region", "ap-southeast-1")
    .option("awsAccessKey", dbutils.secrets.get("aws", "demo_access_key"))
    .option("awsSecretKey", dbutils.secrets.get("aws", "demo_secret_key"))
    .option("encoding", "UTF-8")
    .load()
)

// COMMAND ----------

val df_raw = sdf.selectExpr("CAST(data as STRING) json_data")

// assuming that the JSON string is enclosed within {}
val df_json = df_raw.withColumn("json_col", regexp_extract(col("json_data"), "\\{\"schema\".*\\}", 0))

// COMMAND ----------

val schema = new StructType(Array(
    StructField("schema", StringType, true),
    StructField("data", ArrayType(StructType(Array(
        StructField("e", StringType, true),
        StructField("se_ac", StringType, true),
        StructField("se_pr", StringType, true),
        StructField("se_va", DecimalType(10,2), true),
        StructField("uid", StringType, true),
        StructField("eid", StringType, true),
        StructField("dtm", StringType, true),
        StructField("p", StringType, true),
        StructField("tv", StringType, true),
        StructField("tz", StringType, true),
        StructField("res", StringType, true),
        StructField("stm", StringType, true)
        ))))
))

val df_col = df_json.withColumn("data", from_json(col("json_col"), schema)).select(col("data.*"))
val df = df_col.withColumn("extract_data", explode(col("data"))).select("extract_data.*")


// COMMAND ----------

display(df.limit(20))

// COMMAND ----------

// 
