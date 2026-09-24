# Databricks notebook source
# MAGIC %md
# MAGIC ### **GOLD** **LAYER**

# COMMAND ----------

df=spark.table("silver_flights")
display(df)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Worst Airlines by Delay**

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(col("AIRLINE_NAME").isNull() | col("DEPARTURE_DELAY").isNull()).show()

# COMMAND ----------

display(
    gold_airline_delay.orderBy("avg_departure_delay", ascending=False)
)

# COMMAND ----------

from pyspark.sql.functions import avg, round, count

gold_airline_delay = df.groupBy("AIRLINE_NAME") \
    .agg(count("*").alias("total_flights"),round(avg("DEPARTURE_DELAY"), 2).alias("avg_departure_delay"))

# COMMAND ----------

display( gold_airline_delay.orderBy("avg_departure_delay", ascending=False)
)

# COMMAND ----------

gold_airline_delay.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_airline_delay")

# COMMAND ----------

display(spark.table("gold_airline_delay"))

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(
    col("ORIGIN_AIRPORT").isNull() |
    col("DEPARTURE_DELAY").isNull()
).show()

# COMMAND ----------

from pyspark.sql.functions import avg, round, count

gold_airport_delay = df.groupBy( "ORIGIN_AIRPORT", "ORIGIN_CITY", "ORIGIN_STATE"
).agg(count("*").alias("total_departures"),round(avg("DEPARTURE_DELAY"), 2).alias("avg_departure_delay"))

# COMMAND ----------

display(
    gold_airport_delay.orderBy("avg_departure_delay",ascending=False
    )
)

# COMMAND ----------

gold_airport_delay.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_airport_delay")

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(
    col("day_of_week").isNull() |
    col("hour_of_day").isNull() |
    col("DEPARTURE_DELAY").isNull()
).show()

# COMMAND ----------

from pyspark.sql.functions import avg, round, count

gold_delay_pattern = df.groupBy( "day_of_week", "hour_of_day"
).agg(count("*").alias("total_flights"),round(avg("DEPARTURE_DELAY"), 2).alias("avg_departure_delay")
)

# COMMAND ----------

display(
    gold_delay_pattern.orderBy(
        "day_of_week",
        "hour_of_day"
    )
)

# COMMAND ----------

gold_delay_pattern.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_delay_pattern")

# COMMAND ----------

from pyspark.sql.functions import col

df.filter(
    col("ORIGIN_AIRPORT").isNull() |
    col("DESTINATION_AIRPORT").isNull() |
    col("DEPARTURE_DELAY").isNull() |
    col("ARRIVAL_DELAY").isNull()
).show()

# COMMAND ----------

from pyspark.sql.functions import avg, round, count

gold_route_reliability = df.groupBy(
    "ORIGIN_AIRPORT",
    "DESTINATION_AIRPORT"
).agg(count("*").alias("total_flights"),round(avg("DEPARTURE_DELAY"), 2).alias("avg_departure_delay"),round(avg("ARRIVAL_DELAY"), 2).alias("avg_arrival_delay")
)

# COMMAND ----------

display(
    gold_route_reliability.orderBy(
        "avg_departure_delay",
        ascending=False
    )
)

# COMMAND ----------

gold_route_reliability.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_route_reliability")

# COMMAND ----------

df = spark.table("silver_flights_cancelled")

# COMMAND ----------

print(df.columns)

# COMMAND ----------

display(
    df.select(
        "CANCELLATION_REASON",
        "AIRLINE_NAME",
        "ORIGIN_AIRPORT"
    )
)