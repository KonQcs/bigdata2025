from pyspark.sql import SparkSession
from datetime import datetime
import math

sc = SparkSession \
    .builder \
    .appName("Q1_RDD") \
    .getOrCreate() \
    .sparkContext

username = "kkiousis"
sc.setLogLevel("ERROR")
job_id = sc.applicationId

# 1) Φορτώνουμε το CSV του 2015 ως RDD γραμμών
file_2015 = "mini_yellow_tripdata_2015.csv"
lines = sc.textFile(file_2015)

# 2) Αφαιρούμε το header (πρώτη γραμμή)
header = lines.first()
data = lines.filter(lambda row: row != header)

def parse_line(line):
    parts = line.split(",")
    # index για pickup_latitude=5, pickup_longitude=4 (σύμφωνα με spec)
    try:
        pickup_datetime = parts[1]  # tpep_pickup_datetime
        lat = float(parts[6])       # pickup_latitude
        lon = float(parts[5])       # pickup_longitude
    except:
        return None
    if lat == 0.0 or lon == 0.0:
        return None
    # Παίρνουμε την ώρα:
    dt = datetime.strptime(pickup_datetime, "%Y-%m-%d %H:%M:%S")
    hour = dt.hour
    return (hour, (lat, lon, 1))  # (ώρα, (άθροισμα_lat, άθροισμα_lon, πλήθος))

# 3) Map
mapped = data.map(parse_line).filter(lambda x: x is not None)

# 4) ReduceByKey: συγκεντρώνουμε sums και counts
def seq_op(acc, val):
    sum_lat, sum_lon, cnt = acc
    lat, lon, c = val
    return (sum_lat + lat, sum_lon + lon, cnt + c)

def comb_op(acc1, acc2):
    sum_lat1, sum_lon1, cnt1 = acc1
    sum_lat2, sum_lon2, cnt2 = acc2
    return (sum_lat1 + sum_lat2, sum_lon1 + sum_lon2, cnt1 + cnt2)

reduced = mapped.aggregateByKey((0.0, 0.0, 0), seq_op, comb_op)

# 5) Υπολογίζουμε το μέσο όρο ανά ώρα
averages = reduced.mapValues(lambda triple: (triple[0] / triple[2], triple[1] / triple[2]))

# 6) Ταξινομούμε κατά αλφαβητική αύξουσα ώρα (0–23)
result = averages.sortByKey()

# 7) Αποθήκευση (π.χ. ως CSV στο HDFS)
#output_path = f"hdfs://hdfs-namenode:9000/user/{username}/q1_rdd_result"
#result.map(lambda x: f"{x[0]},{x[1][0]:.6f},{x[1][1]:.6f}").saveAsTextFile(output_path)

# Αν θέλετε να φέρετε τα πρώτα 24 αποτελέσματα στην οθόνη:
print("HourOfDay\tLongitude\tLatitude")
for hour, (avgLat, avgLon) in result.collect():
    print(f"{hour:02d}: {avgLat:.6f}, {avgLon:.6f}")
