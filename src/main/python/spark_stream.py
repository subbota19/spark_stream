# Databricks notebook source
from pyspark import sql
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import *
from matplotlib import pyplot as plt
import os

# COMMAND ----------

session = sql.SparkSession.builder.getOrCreate()

# COMMAND ----------

# sets creds variables to spark session

session.conf.set("fs.azure.account.auth.type.{}.dfs.core.windows.net".format(os.getenv("account_name")),os.getenv("auth_type"))
session.conf.set("fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(os.getenv("account_name")),os.getenv("provider_type"))
session.conf.set("fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(os.getenv("account_name")),os.getenv("client_id"))
session.conf.set("fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(os.getenv("account_name")),os.getenv("client_secret"))
session.conf.set("fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(os.getenv("account_name")),os.getenv("client_endpoint"))
session.conf.set("fs.azure.account.key.subota19.dfs.core.windows.net",os.getenv("account_key"))

# COMMAND ----------

parquet_schema = StructType([
    StructField("address", StringType(), True),
    StructField("avg_tmpr_c", DoubleType(), True),
    StructField("avg_tmpr_f", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("geoHash", StringType(), True),
    StructField("id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("name", StringType(), True),
    StructField("wthr_date", StringType(), True),
])

# COMMAND ----------

# creates stream for extracting parquet files with Auto Loader

df = session.readStream.schema(parquet_schema).parquet(os.getenv("hotel_weather_path_edit"))

# COMMAND ----------

# creates stream for extracting parquet files as file source from testing resource

df = session.readStream.format("cloudFiles").option("cloudFiles.format","parquet").option("includeTimestamp",True).option("cloudFiles.allowOverwrites", "true").schema(parquet_schema).load(os.getenv("hotel_weather_path"))

# COMMAND ----------

#  creates stream for extracting parquet files as file source from main resource

df = session.readStream.schema(parquet_schema).parquet(os.getenv("hotel_weather_path"))

# COMMAND ----------

# applying group by city and wthr_date, aggregation functionality (total distinct hotels, min/max/avg temperature) to df according to the task

calc_DF = df.groupBy("city","wthr_date").agg(
  approx_count_distinct("id").alias("distinct_hotel_count"),
  avg("avg_tmpr_c").alias("avg_avg_tmpr_c"),
  max("avg_tmpr_c").alias("max_avg_tmpr_c"),
  min("avg_tmpr_c").alias("min_avg_tmpr_c"),
  avg("avg_tmpr_f").alias("avg_avg_tmpr_f"),
  max("avg_tmpr_f").alias("max_avg_tmpr_f"),
  min("avg_tmpr_f").alias("min_avg_tmpr_f")
)

# COMMAND ----------

# applying group by city and wthr_date, aggregation functionality (total distinct hotels, min/max/avg temperature) to df according to the task with withWatermark and windowing


calc_DF = df.withColumn("timestamp", current_timestamp()).withWatermark("timestamp","5 minutes").groupBy(window("timestamp", "2 minutes", "1 minutes"),"city","wthr_date").agg(
  approx_count_distinct("id").alias("distinct_hotel_count"),
  avg("avg_tmpr_c").alias("avg_avg_tmpr_c"),
  max("avg_tmpr_c").alias("max_avg_tmpr_c"),
  min("avg_tmpr_c").alias("min_avg_tmpr_c"),
  avg("avg_tmpr_f").alias("avg_avg_tmpr_f"),
  max("avg_tmpr_f").alias("max_avg_tmpr_f"),
  min("avg_tmpr_f").alias("min_avg_tmpr_f")
)

# COMMAND ----------

session.sql("drop table if exists hotel_statistics")

# COMMAND ----------

# write stream to table and save metadata to checkpointLocation for fault-tolerance situation

calc_DF.writeStream.trigger(processingTime='10 seconds').format("delta").outputMode("append").option("checkpointLocation", "dbfs:/hotel_statistics_check").toTable("hotel_statistics")

# COMMAND ----------

# write stream and save in memory for testing purpose

calc_DF.writeStream.outputMode('complete').format("memory").queryName("hotel_statistics").start()

# COMMAND ----------

# creates a list with the biggest city by hotels count, for getting this we need to group by city hotel df and aggregate it with max function after these actions, start to order by aggregation max function and limit it  + transform df to Pandas df for working with matplotlib 

biggest_city = list(session.read.table("hotel_statistics").groupBy("city").agg(max("distinct_hotel_count").alias("max_number_of_hotels")).orderBy(desc("max_number_of_hotels")).limit(10).toPandas().city)

# list with dataframe columns name for displaying it in graphics
statistics_columns = ["avg_avg_tmpr_c","max_avg_tmpr_c","min_avg_tmpr_c","avg_avg_tmpr_f","max_avg_tmpr_f","min_avg_tmpr_f"]

for city in biggest_city:
  
  city_df = hotel_statistics.filter(f'city == "{city}"').toPandas() # fliter dataframe by city from biggest_city list
  
  for graphic in statistics_columns:
    plt.xticks(rotation = 90)
    plt.scatter(city_df.wthr_date, city_df[graphic], label=graphic, color='green')
    plt.xlabel("date of observation")
    plt.ylabel("aggegation function")
    plt.title(f"Hotel Statistics for {city}: {graphic}")
    plt.legend()
    plt.show()

