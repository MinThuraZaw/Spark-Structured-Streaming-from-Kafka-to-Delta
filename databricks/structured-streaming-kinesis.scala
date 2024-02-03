// Databricks notebook source
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

// COMMAND ----------

// %sql
// create or replace table demo_catalog.demo_schema.kinesis_raw_events 
// (
//   id string,
//   timestamp string,
//   device_timezone string,
//   device_screenresolution string,
//   user_id string,
//   app_platform string,
//   event_type string,
//   property string,
//   value string
// )

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

val df_table = df.select(
    col("eid").alias("id"),
    col("dtm").alias("timestamp"),
    col("tz").alias("device_timezone"),
    col("res").alias("device_screenresolution"),
    col("uid").alias("user_id"),
    col("p").alias("app_platform"),
    col("se_ac").alias("event_type"),
    col("se_pr").alias("property"),
    col("se_va").alias("value")
)

// COMMAND ----------

display(df_table.limit(20))

// COMMAND ----------

df_table
    .writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation","demo_catalog.demo_schema.kinesis_raw_events")
    .table("demo_catalog.demo_schema.kinesis_raw_events")

// COMMAND ----------

// 
