-- Databricks notebook source
CREATE RECIPIENT IF NOT EXISTS demo-data-analyst

-- COMMAND ----------

DESCRIBE RECIPIENT demo-data-analyst;

-- COMMAND ----------

--

-- COMMAND ----------

CREATE SHARE IF NOT EXISTS demo_streaming_table

-- COMMAND ----------

ALTER SHARE demo_streaming_table ADD SCHEMA demo_catalog.demo_schema;

-- COMMAND ----------

GRANT SELECT ON SHARE demo_streaming_table TO RECIPIENT demo-data-analyst;

-- COMMAND ----------

SHOW GRANT ON SHARE demo_streaming_table;

-- COMMAND ----------

--
