# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.window import Window

# Initialize a Spark session
spark = SparkSession.builder.appName("CleanMeterData").getOrCreate()

# Load the .dat file
dat_file_path = "InputData/DailyMeterData.dat"
dat_df = spark.read.option("header", "true").csv(dat_file_path)

# Load the CSV file
csv_file_path = "InputData/CustMeter.csv"
csv_df = spark.read.option("header", "true").csv(csv_file_path)

# Filter out rows with "Data Type" values of "Reverse Energy in Wh" or "Net Energy in WH"
# dat_df = dat_df.filter(~(col("Data Type").isin(["Reverse Energy in Wh", "Net Energy in WH"])))
#
# # Filter out rows with QC codes other than '3'
# dat_df = dat_df.filter(col("QC#1") == '3')
#
# # Create a list of columns to keep
# columns_to_keep = [
#     "Meter Number",
#     "Customer Account Number",
#     "Serial Number",
#     "Port",
#     "Channel",
#     "Conversion Factor",
#     "Data Type",
#     "Start Date",
#     "Start Time"
# ]
#
# # Create a list of columns for intervals and QC codes
# interval_columns = [f"Interval#{i}" for i in range(1, 25)]
# qc_columns = [f"QC#{i}" for i in range(1, 25)]
#
# # Create a list of columns for the long format
# long_format_columns = [
#     "IntervalHour",
#     "QCCode",
#     "IntervalValue"
# ]
#
# # Explode the DataFrame to convert wide format to long format
# long_df = dat_df.select(columns_to_keep + interval_columns + qc_columns) \
#     .withColumn("IntervalHour", col("IntervalHour").cast("int")) \
#     .withColumn("long_format", col("Interval#1")).withColumn("long_format", col("long_format").cast("double")) \
#     .select(*columns_to_keep + ["IntervalHour", "long_format"]) \
#     .selectExpr(*columns_to_keep, "stack(24," + ", ".join([f"'{i}', {c}" for i, c in enumerate(qc_columns, 1)]) + ") as (QCCode, IntervalValue)") \
#     .filter(col("IntervalValue").isNotNull()) \
#     .distinct()
#
# # Join with the CSV DataFrame to get "ServiceType"
# result_df = long_df.join(csv_df, on=["Customer Account Number", "Meter Number"], how="inner")
#
# # Sort the DataFrame
# result_df = result_df.orderBy(
#     col("Customer Account Number"),
#     col("Meter Number"),
#     col("Data Type"),
#     col("Start Date"),
#     col("IntervalHour")
# )
#
# # Save the result as a CSV file
# result_df.write.mode("overwrite").csv("/mnt/output/CSV")
#
# # Save the result as a Parquet file
# result_df.write.mode("overwrite").parquet("/mnt/output/Parquet")
#
# # Stop the Spark session
# spark.stop()
