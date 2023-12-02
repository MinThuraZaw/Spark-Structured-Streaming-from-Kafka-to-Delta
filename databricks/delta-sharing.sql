-- Databricks notebook source
CREATE RECIPIENT IF NOT EXISTS demo-data-analyst

-- COMMAND ----------

DESCRIBE RECIPIENT demo-data-analyst;

-- COMMAND ----------

--

-- COMMAND ----------

CREATE SHARE IF NOT EXISTS demo_streaming_kinesis
