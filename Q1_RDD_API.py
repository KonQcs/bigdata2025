from pyspark import SparkContext
from datetime import datetime

sc = SparkContext(appName="Q1_RDD")

lines = sc.textFile("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

header = lines.first()
data  = lines.filter(lambda row: row != header)

def parse_line_to_hour_latlon(line):
    fields = line.split(",")
    try:
        pickup_datetime = fields[1]
        lat = float(fields[6])
        lon = float(fields[7])
        if lat == 0.0 or lon == 0.0:
            return None
        hour = datetime.strptime(pickup_datetime, "%Y-%m-%d %H:%M:%S").hour
        return hour, (lat, lon, 1)
    except:
        return None

records = data.map(parse_line_to_hour_latlon).filter(lambda x: x is not None)
sums = records.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2]))
avg_per_hour = sums.mapValues(lambda v: (v[0]/v[2], v[1]/v[2]))
sorted_result = avg_per_hour.sortByKey()

sorted_result.map(lambda x: f"{x[0]},{x[1][0]:.6f},{x[1][1]:.6f}") \
             .saveAsTextFile("hdfs://hdfs-namenode:9000/user/kkiousis/output/Q1/rdd/")

sc.stop()
