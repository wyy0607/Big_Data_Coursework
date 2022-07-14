from pyspark import SparkContext
import pandas as pd
import scipy.stats as stats
import re

# load data
sc = SparkContext()
input = sc.textFile("hdfs://wolf:9000/user/ywl3922/hw2/data/crime/Crimes_-_2001_to_present.csv")
# split data
input = input.map(lambda line: re.split(r',(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)', line))
# get header and data
header = input.first()
data = input.filter(lambda row: row != header)

# part 1

block_ind = header.index("Block")
year_ind = header.index("Year")
top10_crime_blocks = sorted(data.map(lambda x: (x[block_ind][0:5], x[year_ind]))\
    .filter(lambda x: x[1] in ["2020", "2019", "2018"])\
    .map(lambda x: (x[0], 1))\
    .countByKey().items(), key=lambda x: x[1], reverse=True)[0:10]
# save output for part 1
text_file = open("Wu_2.txt", "w")
text_file.write("1. top 10 blocks in crime events in the last 3 years \n")
for i in range(10):
    text_file.write(str(top10_crime_blocks[i]) + "\n")

# part 2

# get yearly crime counts for each beat
beat_ind = header.index('Beat')
beat_crime_counts = data.map(lambda x: (x[beat_ind], x[year_ind]))\
    .filter(lambda x:  x[1] in ["2020", "2019", "2018", "2017", "2016"])\
    .map(lambda x: (x[0] + "in" + x[1], 1))\
    .reduceByKey(lambda a, b: a + b)\
    .map(lambda x: (x[0][0:4], x[0][6:10], x[1]))\
    .sortBy(lambda x: (x[0], x[1]))\
    .map(lambda x: (x[0], [x[2]]))\
    .reduceByKey(lambda a, b: a + b)\
    .collectAsMap()
# convert the dict to dataframe
crime_df = pd.DataFrame.from_dict(beat_crime_counts, orient='index').T
# compute correlation
crime_corr = crime_df.corr()
# combine correlated beats into dictionary
high_corr_beats = {}
for row in crime_corr.index:
    for col in crime_corr.columns:
        if row<col:
            high_corr_beats[row+","+col] = crime_corr[row][col]
# sort by correlation coefficients
high_corr_beats = sorted(high_corr_beats.items(), key=lambda x:x[1], reverse=True)
# save output for part 2
text_file.write("2. two beats that are adjacent with the highest correlation \n")
text_file.write("After checking the website, the two beats are 0714 and 0825. \n")
text_file.write("0714 and 0825 are the beats pair with 17th highest correlation,")
text_file.write("but the top 16 pairs are not adjacent. \n")
for i in range(20):
    text_file.write(str(high_corr_beats[i]) + "\n")

# part 3

# Mayor Daly 1989-2011, Mayor Emanuel 2011-2019
# check if total number of crime over districts for the last three years for the two mayors differ
# despite year 2011 and 2019 since they did not in charge for the whole year
daly_year = ["2010", "2009", "2008"]
emanuel_year = ["2018", "2017", "2016"]
district_ind = header.index("District")
daly_avg = data.map(lambda x: (x[district_ind], x[year_ind]))\
    .filter(lambda x: x[1] in daly_year)\
    .map(lambda x: (x[0], 1))\
    .countByKey()
emanuel_avg = data.map(lambda x: (x[district_ind], x[year_ind]))\
    .filter(lambda x: x[1] in emanuel_year)\
    .map(lambda x: (x[0], 1))\
    .countByKey()

# get values sorted by district
daly_values = [value for (_, value) in sorted(daly_avg.items(), reverse=True)]
emanuel_values = [value for (_, value) in sorted(emanuel_avg.items(), reverse=True)]

# compute paired t-test score
t_val = stats.ttest_rel(daly_values, emanuel_values)[0]
p_val = stats.ttest_rel(daly_values, emanuel_values)[1]

# save output for part 3
text_file.write("3. establish if the number of crime events "
"is different between Mayors Daly and Emanuel \n")
text_file.write("For this part, I compared the number of crime events for the last three years"
"the two mayors in charge per district. \n")
text_file.write("The test score for paired t-test is: " + str(t_val) + "\n")
text_file.write("The p-value for the paired t-test is: "+ str(p_val) + "\n")
text_file.write("Therefore, the number of crime events for the two mayors are different.")
text_file.close()
