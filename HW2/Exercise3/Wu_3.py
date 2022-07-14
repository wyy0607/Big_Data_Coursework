from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

# load data 
sc = SparkContext()
sqlcontext = SQLContext(sc)
path = "hdfs://wolf:9000/user/ywl3922/hw2/data/crime/Crimes_-_2001_to_present.csv"
data = sqlcontext.read.csv(path, header=True)

# add column to identify it the row represents violent crime or not
violent = data.withColumn("Violent", when(col("IUCR").like("01%") |\
    col("IUCR").like("02%") | col("IUCR").like("03%") | col("IUCR").like("04%") |\
    col("IUCR").like("05%") | col("IUCR").like("06%"), 1).otherwise(0))

#add week and cast year to int
weekly_violent = violent.withColumn("Time", to_timestamp("Date","MM/dd/yyyy hh:mm:ss a"))\
    .withColumn("Week", weekofyear(col("Time")))\
    .withColumn("Year", col("Year").cast(IntegerType()))

# encode arrest and domestic
weekly_violent = weekly_violent \
    .withColumn("Arrest", when(col("Arrest") == "true", 1)\
        .otherwise(0))\
    .withColumn("Domestic", when(col("Domestic") == "true", 1)\
        .otherwise(0))

# aggregate at beat level
agg_violent = weekly_violent.groupBy("Year", "Week", "Beat")\
    .agg(sum(col("Arrest")), sum(col("Domestic")), sum(col("Violent")), count("ID"))\
    .orderBy("Year", "Week", "Beat")\
    .drop("sum(Week)")

# add previous crime count, arrest count, violent  count, and domestic count
agg_violent2 = agg_violent\
       .withColumn("prev_crime", lag("count(ID)").\
              over(Window.partitionBy("Beat").orderBy("Year", "Week")))\
       .withColumn("prev_prev_crime", lag("count(ID)", 2).\
              over(Window.partitionBy("Beat").orderBy("Year", "Week")))\
       .withColumn("prev_arrest", lag("sum(Arrest)").\
              over(Window.partitionBy("Beat").orderBy("Year", "Week")))\
       .withColumn("prev_prev_arrest", lag("sum(Arrest)", 2).\
              over(Window.partitionBy("Beat").orderBy("Year", "Week")))\
       .withColumn("prev_violent", lag("sum(Violent)").\
              over(Window.partitionBy("Beat").orderBy("Year", "Week")))\
       .withColumn("prev_prev_violent", lag("sum(Violent)", 2).\
              over(Window.partitionBy("Beat").orderBy("Year", "Week")))\
       .withColumn("prev_domestic", lag("sum(Domestic)").\
              over(Window.partitionBy("Beat").orderBy("Year", "Week")))\
       .withColumn("prev_prev_domestic", lag("sum(Domestic)", 2).\
              over(Window.partitionBy("Beat").orderBy("Year", "Week")))

# drop current arrest count, violent  count, and domestic for predicting all crimes
all_crime_df = agg_violent2.drop("sum(Arrest)", "sum(Domestic)", "sum(Violent)")

# drop current crime count, arrest count, and domestic count for predicting violent
violent_df = agg_violent2.drop("sum(Arrest)", "sum(Domestic)", "count(ID)")

# drop rows that contain missing values
all_crime_df = all_crime_df.na.drop()
violent_df = violent_df.na.drop()

# Model Pipeline

BeatIdx = StringIndexer(inputCol='Beat', outputCol='BeatIdx')
WeekIdx = StringIndexer(inputCol='Week', outputCol='WeekIdx')
encoder = OneHotEncoder(inputCols = ["BeatIdx","WeekIdx"], 
    outputCols = ["BeatVec","WeekVec"]).setHandleInvalid("keep")
assembler = VectorAssembler(inputCols=["BeatVec", "WeekVec", "Year", 
    "prev_crime", "prev_prev_crime", "prev_arrest", "prev_prev_arrest",
    "prev_violent", "prev_prev_violent", "prev_domestic", "prev_prev_domestic"], 
    outputCol='features')
crime_rf_model = RandomForestRegressor(labelCol="count(ID)", featuresCol="features")
crime_pipeline = Pipeline(stages=[BeatIdx, WeekIdx, encoder, assembler, crime_rf_model])

violent_rf_model = RandomForestRegressor(labelCol="sum(Violent)", featuresCol="features")
violent_pipeline = Pipeline(stages=[BeatIdx, WeekIdx, encoder, assembler, 
    violent_rf_model])

# Model Fit
crime_train, crime_test = all_crime_df.randomSplit([0.8, 0.2])
violent_train, violent_test = violent_df.randomSplit([0.8, 0.2])

crime_model = crime_pipeline.fit(crime_train)
crime_predictions = crime_model.transform(crime_test)

violent_model = violent_pipeline.fit(violent_train)
violent_predictions = violent_model.transform(violent_test)

# model evaluation
crime_pred = crime_predictions.select(col("count(ID)").cast("Float"), col("prediction"))
crime_evaluator = RegressionEvaluator(labelCol="count(ID)", predictionCol="prediction", 
    metricName="rmse")
crime_rmse = crime_evaluator.evaluate(crime_pred)
print(crime_rmse)

violent_pred = violent_predictions.select(col("sum(Violent)").cast("Float"), 
    col("prediction"))
violent_evaluator = RegressionEvaluator(labelCol="sum(Violent)", 
    predictionCol="prediction", metricName="rmse")
violent_rmse = violent_evaluator.evaluate(violent_pred)
print(violent_rmse)

text_file = open("Wu_3.txt", "w")
text_file.write("RMSE for predicting the number of all crime events in "
    "the next week at the beat level: " + str(crime_rmse) + "\n")
text_file.write("RMSE for predicting the number of violent crime events in "
    "the next week at the beat level: " + str(violent_rmse) + "\n")
text_file.close()
