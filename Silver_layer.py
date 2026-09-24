# Databricks notebook source
# MAGIC %md
# MAGIC ### **Silver Layer**

# COMMAND ----------

df=spark.table("bronze_flight")
display(df)
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Parse YEAR/MONTH/DAY inot a real** **DATE**

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col

df = df.withColumn( "REAL_DATE", F.make_date(col("YEAR").cast("int"),col("MONTH").cast("int"),col("DAY").cast("int"))
)

# COMMAND ----------

display(df)

# COMMAND ----------

df.select("YEAR", "MONTH", "DAY", "REAL_DATE").show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Convert HHMM integers into proper **timestamps**

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col

df = df.withColumn( "REAL_DATE", F.make_date(col("YEAR").cast("int"), col("MONTH").cast("int"),col("DAY").cast("int"))
)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col

def hhmm_to_timestamp(df, column_name):

    return df.withColumn( column_name, F.when( col(column_name).isNull(), None).when(
            col(column_name) == 2400,F.to_timestamp( F.concat_ws( " ", col("REAL_DATE"), F.lit("00:00:00"))))

        .otherwise(F.to_timestamp( F.concat_ws( " ",col("REAL_DATE"), F.concat( F.substring( F.lpad(col(column_name).cast("string"),4,"0"),  1, 2 ), F.lit(":"), F.substring( F.lpad(col(column_name).cast("string"),4,"0"), 3, 2 ),  F.lit(":00") ))) )

    )

# COMMAND ----------

hhmm_columns = [ "SCHEDULED_DEPARTURE", "DEPARTURE_TIME", "WHEELS_OFF", "WHEELS_ON", "SCHEDULED_ARRIVAL", "ARRIVAL_TIME"]

for c in hhmm_columns:df = hhmm_to_timestamp(df, c)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql.functions import col
cancelled_df=df.filter(col("CANCELLED")== 1)
complete_df=df.filter(col("CANCELLED")== 0)
display(cancelled_df)



# COMMAND ----------

cancelled_df.count()

# COMMAND ----------

complete_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## **JOIN**

# COMMAND ----------

airports_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/us_dot_flights/us_dot_flights/us_dot_flights/airports.csv")

# COMMAND ----------

display(airports_df)

# COMMAND ----------

airlines_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/us_dot_flights/us_dot_flights/us_dot_flights/airlines (1).csv")

# COMMAND ----------

display(airlines_df)

# COMMAND ----------

complete_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col
airlines_df = airlines_df.withColumnRenamed("AIRLINE","AIRLINE_NAME"
)

# COMMAND ----------

# complete_df = complete_df.join(
#     airlines_df,
#     complete_df["AIRLINE"] == airlines_df["IATA_CODE"],
#     "left"
# )

# COMMAND ----------

from pyspark.sql.functions import col 
complete_df = df.filter(col("CANCELLED") == 0)

complete_df.printSchema()

# COMMAND ----------

airlines_df.printSchema()

# COMMAND ----------

complete_df = complete_df.join(airlines_df,complete_df["AIRLINE"] == airlines_df["IATA_CODE"],"left"
)

# COMMAND ----------

complete_df = complete_df.drop("IATA_CODE")

# COMMAND ----------

display(complete_df)

# COMMAND ----------

origin_airport = airports_df.alias("origin")

# COMMAND ----------

complete_df = complete_df.join( f, complete_df["ORIGIN_AIRPORT"] == col("origin.IATA_CODE"), "left")

# COMMAND ----------

from pyspark.sql.functions import col
complete_df = complete_df.withColumn("ORIGIN_CITY",col("origin.CITY"))

# COMMAND ----------

from pyspark.sql.functions import col

complete_df = complete_df \
    .withColumn("ORIGIN_CITY", col("origin.CITY")) \
         .withColumn("ORIGIN_STATE", col("origin.STATE")) \
         .withColumn("ORIGIN_LATITUDE", col("origin.LATITUDE")) \
              .withColumn("ORIGIN_LONGITUDE", col("origin.LONGITUDE"))

# COMMAND ----------

display(complete_df)

# COMMAND ----------

destination_airport = airports_df.alias("destination")

# COMMAND ----------

complete_df = complete_df.join( destination_airport, complete_df["DESTINATION_AIRPORT"] == col("destination.IATA_CODE"), "left")

# COMMAND ----------

from pyspark.sql.functions import col

complete_df = complete_df \
    .withColumn("DEST_CITY", col("destination.CITY")) \
    .withColumn("DEST_STATE", col("destination.STATE")) \
    .withColumn("DEST_LATITUDE", col("destination.LATITUDE")) \
    .withColumn("DEST_LONGITUDE", col("destination.LONGITUDE"))

# COMMAND ----------

from pyspark.sql import functions as F

complete_df = complete_df.withColumn( "day_of_week", F.date_format("REAL_DATE", "EEEE")
)

# COMMAND ----------

from pyspark.sql import functions as F

complete_df = complete_df.withColumn( "hour_of_day", F.hour("SCHEDULED_DEPARTURE"))

# COMMAND ----------

from pyspark.sql import functions as F

complete_df = complete_df.withColumn( "is_delayed", F.when(F.col("DEPARTURE_DELAY") > 15, True)  .otherwise(False)
)

# COMMAND ----------

from pyspark.sql import functions as F

complete_df = complete_df.withColumn(
    "delay_bucket",
    F.when(F.col("DEPARTURE_DELAY") <= 15, "On Time")
     .when((F.col("DEPARTURE_DELAY") > 15) & (F.col("DEPARTURE_DELAY") <= 30), "Minor Delay")
     .when((F.col("DEPARTURE_DELAY") > 30) & (F.col("DEPARTURE_DELAY") <= 60), "Major Delay")
     .otherwise("Severe Delay")
)

# COMMAND ----------

from collections import Counter

duplicates = [c for c, count in Counter(complete_df.columns).items() if count > 1]
print(duplicates)

# COMMAND ----------

complete_df = complete_df.drop("IATA_CODE","AIRPORT","CITY","STATE","COUNTRY","LATITUDE","LONGITUDE")

# COMMAND ----------

complete_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_flights")

# COMMAND ----------

cancelled_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_flights_cancelled")

# COMMAND ----------

display("cancelled_df")

# COMMAND ----------

cancelled_df = cancelled_df.join( airlines_df, cancelled_df.AIRLINE == airlines_df.IATA_CODE, "left")

# COMMAND ----------

display(cancelled_df)

# COMMAND ----------

cancelled_df = cancelled_df.drop("IATA_CODE")

# COMMAND ----------

cancelled_df = cancelled_df.join(
    airports_df.select(
        col("IATA_CODE").alias("ORIGIN_CODE"),
        col("CITY").alias("ORIGIN_CITY"),
        col("STATE").alias("ORIGIN_STATE"),
        col("LATITUDE").alias("ORIGIN_LATITUDE"),
        col("LONGITUDE").alias("ORIGIN_LONGITUDE")
    ),
    cancelled_df.ORIGIN_AIRPORT == col("ORIGIN_CODE"),
    "left"
).drop("ORIGIN_CODE")

# COMMAND ----------

cancelled_df = cancelled_df.join(
    airports_df.select(
        col("IATA_CODE").alias("DEST_CODE"),
        col("CITY").alias("DEST_CITY"),
        col("STATE").alias("DEST_STATE"),
        col("LATITUDE").alias("DEST_LATITUDE"),
        col("LONGITUDE").alias("DEST_LONGITUDE")
    ),
    cancelled_df.DESTINATION_AIRPORT == col("DEST_CODE"),
    "left"
).drop("DEST_CODE")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS silver_flights_cancelled;

# COMMAND ----------

cancelled_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_flights_cancelled")