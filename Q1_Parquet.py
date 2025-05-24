from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, avg, col
import time

spark = SparkSession.builder.appName("Q1_Parquet").getOrCreate()

start = time.time()

df_parquet = spark.read.parquet("hdfs://hdfs-namenode:9000/user/kkiousis/data/parquet/yellow_tripdata_2015.parquet")
df_nz = df_parquet.filter((col("pickup_latitude") != 0.0) & (col("pickup_longitude") != 0.0))

df_res = df_nz.withColumn("hour", hour(col("tpep_pickup_datetime"))) \
    .groupBy("hour") \
    .agg(
        avg(col("pickup_latitude")).alias("avg_latitude"),
        avg(col("pickup_longitude")).alias("avg_longitude")
    ) \
    .orderBy("hour")

df_res.collect()

end = time.time()
print(f"SparkSQL on Parquet took {end - start:.2f} seconds")

spark.stop()
