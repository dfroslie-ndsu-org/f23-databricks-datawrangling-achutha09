# Databricks notebook source
from pyspark.sql.functions import sum, max
from pyspark.sql.types import IntegerType
uri = "abfss://assign1@achuthastorage.dfs.core.windows.net/"
storage_end_point = "achuthastorage.dfs.core.windows.net"
my_scope = "databricksSecretScope"
my_key = "storage-keyvault"
spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))
df=spark.read.parquet(uri+"Output/Parquet")
csvDataFile = spark.read.csv(uri+'Output/CustMeter.csv', header=True)



# 1. What's the total electrical usage for the day?
from pyspark.sql import functions as F
sum_result = df.select(F.sum("IntervalValue")).first()[0]
print("1. What's the total electrical usage for the day?")
print(sum_result)

# What's the total electrical usage for the day?
# 117867.07430000443



# 2. What's the total electrical usage for 'Residental' customers for the day?
residentialUsage = df.filter(df["ServiceType"] == "Residential").agg(F.sum("IntervalValue")).collect()[0][0]
print("2. What's the total electrical usage for 'Residental' customers for the day?")
display(residentialUsage)

# What's the total electrical usage for 'Residental' customers #for the day?
# 103003.62940000501

# COMMAND ----------

# 3. What's the total electrical usage for hour 7 of the day?
seventhHourUsage = df.filter(df["IntervalHour"] == "7").agg(F.sum("IntervalValue")).collect()[0][0]
print("3. What's the total electrical usage for hour 7 of the day?")
display(seventhHourUsage)

# What's the total electrical usage for hour 7 of the day?
# Answer:4538.916000000025

# COMMAND ----------

# Find the top 5 meters with the highest total usage
meter_usage = df.groupBy("Meter Number").agg(F.sum("IntervalValue").alias("TotalUsage"))
top_5_meters = meter_usage.orderBy(F.desc("TotalUsage")).limit(5)

# Collect the results into a list

top_meters_list = top_5_meters.collect()
print("The below table displays the top five meters with highest total usage value")
display(top_meters_list)

# Meter Number	TotalUsage
# 13273207	1619.2049000000002
# 10264378	1601.676
# 12461706	1429.8359999999998
# 10264358	1376.5860000000002
# 10264370	1341.9899999999998


# 5. Which hour had the most usage for the day and what was the total electrical usage?
filtered_data = df.select("IntervalHour", df["IntervalValue"].cast(IntegerType()).alias("IntervalValue"))

hourly_usage = filtered_data.groupBy("IntervalHour").agg(sum("IntervalValue").alias("TotalUsage"))

# Find the hour with the highest total usage
max_hour = hourly_usage.select("IntervalHour").filter(hourly_usage["TotalUsage"] == hourly_usage.agg(max("TotalUsage")).collect()[0][0]).collect()[0][0]

# Find the total electrical usage for the hour with the highest total usage
max_usage = hourly_usage.agg(max("TotalUsage")).collect()[0][0]
print("5. Which hour had the most usage for the day and what was the total electrical usage?")
print("The hour with maximum usage is: "+str(max_hour))
print("The total energy usage was : "+str(max_usage))

# 5. Which hour had the most usage for the day and what was the #total electrical usage?
# The hour with maximum usage is: 21
# The total energy usage was : 4776


# The databricks is reading the parquet file which is split into two different files
# 6. How many meters are in CustMeter.csv dataset that didn't have any valid readings for the day after cleaning the data?
countParquet=df.select("Meter Number").distinct().count()
countCustMeter=csvDataFile.select("Meter Number").distinct().count()
print("6.How many meters are in CustMeter.csv dataset that didn't have any valid readings for the day after cleaning the data?")
print(countCustMeter-countParquet)

# 6.How many meters are in CustMeter.csv dataset that didn't have any valid readings for the day after cleaning the data?
# Answer: 234
