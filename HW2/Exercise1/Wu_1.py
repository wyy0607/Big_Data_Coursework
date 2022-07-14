from pyspark.sql import SQLContext
from pyspark.sql.functions import substring
from pyspark import SparkContext
import matplotlib.pyplot as plt

# load data 
sc = SparkContext()
sqlcontext = SQLContext(sc)
path = "hdfs://wolf:9000/user/ywl3922/hw2/data/crime/Crimes_-_2001_to_present.csv"
data = sqlcontext.read.csv(path, header=True)

# add column Month
month = data.withColumn("Month", substring("Date", 0, 2))

# calculate monthly average number of crimes
monthly_avg = month.groupby("Year", "Month").count()\
    .groupby("Month").avg().orderBy("Month")

# create barplot
monthly_avg.toPandas().plot(kind="bar", x="Month", y="avg(count)",
xlabel="Month", ylabel="Average Number of Crimes", legend=None,
title="Monthly Average Number of Crimes from 2001")

#save output 
plt.savefig("Wu_1.png")
