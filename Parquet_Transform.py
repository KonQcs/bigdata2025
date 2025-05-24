from pyspark.sql import SparkSession
import time

spark = SparkSession.builder.appName("Convert2015ToParquet").getOrCreate()

df_2015 = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

start_time = time.time()
df_2015.write.parquet("hdfs://hdfs-namenode:9000/user/kkiousis/data/parquet/yellow_tripdata_2015.parquet")
elapsed = time.time() - start_time
print(f"Time to convert CSV → Parquet: {elapsed:.2f} seconds")

spark.stop()
