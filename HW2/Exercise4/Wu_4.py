from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark import SparkContext
import matplotlib.pyplot as plt

# load data 
sc = SparkContext()
sqlcontext = SQLContext(sc)
path = "hdfs://wolf:9000/user/ywl3922/hw2/data/crime/Crimes_-_2001_to_present.csv"
data = sqlcontext.read.csv(path, header=True)

# filter by Arrest
arrestdata = data.filter(data.Arrest == "true")

# 1. time of day

# Change Date time timestamp to MM/dd/yyyy hh:mm:ss
day = arrestdata.withColumn("Time", to_timestamp("Date","MM/dd/yyyy hh:mm:ss a"))\
.withColumn("Day", to_date("Time", "MM/dd/yyyy"))
hour = day.withColumn("Hour", hour(col("Time")))
# calculate hourly average number of arrests
hourly_avg = hour.groupby("Day", "Hour").count()\
    .groupby("Hour").avg().orderBy("Hour")
# create barplot
hourly_avg.toPandas().plot(kind="bar", x="Hour", y="avg(count)",
xlabel="Hour", ylabel="Average Number of Arrests", legend=None,
title="Hourly Average Number of Arrests from 2001")
#save output 
plt.savefig("Wu_4_1.png")

# 2. day of week

# add day of week
week = day.withColumn("Week", weekofyear(col("Time")))\
    .withColumn("day_of_week", dayofweek(col("Time")))
# calculate daily average number of arrests
daily_avg = week.groupby("Year", "Week", "day_of_week").count()\
    .groupby("day_of_week").avg().orderBy("day_of_week")
# create barplot
daily_avg.toPandas().plot(kind="bar", x="day_of_week", y="avg(count)",
xlabel="day of week", ylabel="Average Number of Arrests", legend=None,
title="Daily Average Number of Arrests from 2001")
#save output 
plt.savefig("Wu_4_2.png")

# 3. Month

# add month 
month = day.withColumn("Month", month(col("Time")))
# calculate monthly average number of arrests
monthly_avg = month.groupby("Year", "Month").count()\
    .groupby("Month").avg().orderBy("Month")
# create barplot
monthly_avg.toPandas().plot(kind="bar", x="Month", y="avg(count)",
xlabel="Month", ylabel="Average Number of Arrests", legend=None,
title="Monthly Average Number of Arrests from 2001")
#save output 
plt.savefig("Wu_4_3.png")
