# Databricks notebook source
# MAGIC %md
# MAGIC ### **US DOT Flight Delays** _**Bronze Layer**_

# COMMAND ----------

df=spark.read.format("csv") \
.option("header", "true") \
.option("inferSchema", "true") \
.load("/Volumes/us_dot_flights/us_dot_flights/us_dot_flights/flights.csv")

# COMMAND ----------

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_flight")    


# COMMAND ----------

display(df)