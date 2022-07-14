from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

import pandas as pd

sc = SparkContext()
sc.setLogLevel("ERROR")
sqlcontext = SQLContext(sc)

# Read data
data = sqlcontext.read.csv("s3://msia-431-hw-data/full_data.csv", header = True)

# Cast columns to integer
for c in data.columns[1:]:
    data = data.withColumn(c, f.col(c).cast("Integer"))

# Create columns for year and month
data = data.withColumn("year", f.year(f.to_timestamp("time_stamp", format="yyyy-MM-dd HH:mm:ss"))) \
.withColumn("month", f.month(f.to_timestamp("time_stamp", format="yyyy-MM-dd HH:mm:ss"))) 

# Create column for bar group
data = data.withColumn("bar_group", ((f.col("bar_num") - 1) / 10).cast("Integer"))

# Sort by trade id and bar num
data = data.orderBy(["trade_id", "bar_num"], ascending=[True, True])

# Get additional features
w = Window().partitionBy("trade_id", "bar_group").orderBy("bar_group")
add_features = data.select("trade_id", "bar_group", "bar_num", "profit",
                           f.lag("profit", 1).over(w).alias("lag1_profit"),
                           f.lag("profit", 2).over(w).alias("lag2_profit"),
                           f.lag("profit", 3).over(w).alias("lag3_profit"),
                           f.mean(f.col("profit")).over(w).alias("mean_profit"),
                           f.stddev(f.col("profit")).over(w).alias("std_profit"),
                           f.min(f.col("profit")).over(w).alias("min_profit"),
                           f.max(f.col("profit")).over(w).alias("max_profit")) \
                    .filter(f.col("bar_num") % 10 == 0) \
                    .orderBy(["trade_id", "bar_group"], ascending=[True, True])

# Merge with original data
add_features = add_features.withColumn("next_bar", f.col("bar_group") + 1) \
              .select("trade_id", "next_bar", "lag1_profit", "lag2_profit", "lag3_profit", "mean_profit", 
              "std_profit", "min_profit", "max_profit") \
              .withColumnRenamed("trade_id", "a_trade_id")
merged_data = add_features.join(data, on=(add_features.next_bar == data.bar_group) & (add_features.a_trade_id == data.trade_id), 
              how="right")

# Keep bar_num > 10
final_data = merged_data.filter(f.col("bar_num") > 10)

# Drop rows with missing values
final_data = final_data.na.drop()

#Build model pipeline
target = "profit"
features = ["bar_num", "var12", "var13", "var14", "var15", "var16", "var17", "var18", "var23", "var24", "var25",
                "var26", "var27", "var28", "var34", "var35", "var36", "var37", "var38", "var45", "var46", "var47",
                "var48", "var56", "var57", "var58", "var67", "var68", "var78", "lag1_profit", "lag2_profit", 
                "lag3_profit", "mean_profit", "std_profit", "min_profit", "max_profit"]

assembler = VectorAssembler(inputCols=features, outputCol="features").setHandleInvalid("skip")
rf = RandomForestRegressor(labelCol="profit", featuresCol="features")
pipeline = Pipeline(stages=[assembler, rf])

# Evaluate
train_date = []
test_date = []
mapes = []
for year in range(2008, 2015):
    train = final_data.filter(f.col("year") == year).filter((f.col("month") >= 1) & (f.col("month") <= 6))     
    test = final_data.filter(f.col("year") == year).filter(f.col("month") == 7)
    fitted = pipeline.fit(train)
    pred = fitted.transform(test)
    pred = pred.withColumn("ape", 100 * f.abs((pred["profit"]-pred["prediction"])/pred["profit"]))
    mape = pred.select(f.mean(pred["ape"]).alias("mape")).collect()
    mape = mape[0]["mape"]
    train_date.append("{}-1 to 6".format(year))
    test_date.append("{}-7".format(year))
    mapes.append(mape)

    train = final_data.filter(f.col("year") == year).filter((f.col("month") >= 7) & (f.col("month") <= 12))
    test = final_data.filter(f.col("year") == year+1).filter(f.col("month") == 1)
    fitted = pipeline.fit(train)
    pred = fitted.transform(test)
    pred = pred.withColumn("ape", 100 * f.abs((pred["profit"]-pred["prediction"])/pred["profit"]))
    mape = pred.select(f.mean(pred["ape"]).alias("mape")).collect()
    mape = mape[0]["mape"]
    train_date.append("{}-7 to 12".format(year))
    test_date.append("{}-1".format(year+1))
    mapes.append(mape)

train = final_data.filter(f.col("year") == 2015).filter((f.col("month") >= 1) & (f.col("month") <= 6)) 
test = final_data.filter(f.col("year") == 2015).filter(f.col("month") == 7)
fitted = pipeline.fit(train)
pred = fitted.transform(test)
pred = pred.withColumn("ape", 100 * f.abs((pred["profit"]-pred["prediction"])/pred["profit"]))
mape = pred.select(f.mean(pred["ape"]).alias("mape")).collect()
mape = mape[0]["mape"]
train_date.append("2015-1 to 6")
test_date.append("2015-7")
mapes.append(mape)

#Output
results = pd.DataFrame({"train_date": train_date, "test_date": test_date, "mape": mapes})

avg_mape = results["mape"].mean()
min_mape = results["mape"].min()
max_mape = results["mape"].max()
summary = pd.DataFrame({"avg_mape": [results["mape"].mean()], "min_mape": [results["mape"].min()], 
"max_mape": [results["mape"].max()]})  

results.to_csv("s3://yuyan-big-data-2022/exercise1.txt", header=True, index=False, sep=",")
summary.to_csv("s3://yuyan-big-data-2022/exercise1.txt", header=True, index=False, sep=",", mode="a")

sc.stop()
