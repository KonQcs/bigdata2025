from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, avg, col

spark = SparkSession.builder.appName("Q1_DataFrame").getOrCreate()

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

df_nonzero = df.filter((col("pickup_latitude") != 0.0) & (col("pickup_longitude") != 0.0))
df_with_hour = df_nonzero.withColumn("hour", hour(col("tpep_pickup_datetime")))

result_df = df_with_hour.groupBy("hour") \
    .agg(
        avg(col("pickup_longitude")).alias("avg_longitude"),
        avg(col("pickup_latitude")).alias("avg_latitude")
    ) \
    .orderBy("hour")

result_df.select("hour", "avg_latitude", "avg_longitude") \
         .coalesce(1) \
         .write \
         .option("header", "true") \
         .csv("hdfs://hdfs-namenode:9000/user/kkiousis/output/Q1/dataframe/")

spark.stop()
