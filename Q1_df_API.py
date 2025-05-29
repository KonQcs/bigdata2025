from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, DoubleType
from pyspark.sql.functions import col, hour, avg

username = "kkiousis"
spark = SparkSession \
    .builder \
    .appName("Q1 DataFrame execution (no UDF)") \
    .getOrCreate()
sc = spark.sparkContext

sc.setLogLevel("ERROR")

df = spark.read \
    .option("header", "true") \
    .csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv") \
    .select(
        col("tpep_pickup_datetime").alias("pickup_dt"),
        col("pickup_latitude").cast(DoubleType()),
        col("pickup_longitude").cast(DoubleType())
    )

# Φιλτράρουμε μηδενικές
df_filtered = df.filter((col("pickup_latitude") != 0.0) & (col("pickup_longitude") != 0.0))

# Ώρα και ομαδοποίηση
df_hourly_avg = df_filtered \
    .withColumn("hour", hour(col("pickup_dt"))) \
    .groupBy("hour") \
    .agg(
        avg("pickup_latitude").alias("avg_latitude"),
        avg("pickup_longitude").alias("avg_longitude")
    ) \
    .orderBy("hour")

df_hourly_avg.write.option("header", "true").csv(f"hdfs://hdfs-namenode:9000/user/{username}/q1_df_no_udf_result")
