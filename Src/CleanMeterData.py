from pyspark.sql.functions import substring
from pyspark.sql.functions import  when, lit, asc

uri = "abfss://assign1@achuthastorage.dfs.core.windows.net/"
storage_end_point = "achuthastorage.dfs.core.windows.net"
my_scope = "databricksSecretScope"
my_key = "storage-keyvault"
storage="abfss://assign1@achuthastorage.dfs.core.windows.net/Output/"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

common_columns = ['Customer Account Number', 'Meter Number', 'ServiceType', 'DT', 'Serial Number', 'Port', 'Channel', 'Conversion Factor', 'Data Type', 'Start Date', 'Start Time']

QC_columns = ["QC#1", "QC#2", "QC#3", "QC#4", "QC#5", "QC#6", "QC#7", "QC#8", "QC#9", "QC#10","QC#11", "QC#12", "QC#13", "QC#14", "QC#15", "QC#16", "QC#17", "QC#18", "QC#19", "QC#20"
    ,"QC#21", "QC#22", "QC#23", "QC#24"]

Interval_columns = ["Interval#1", "Interval#2", "Interval#3", "Interval#4", "Interval#5", "Interval#6", "Interval#7", "Interval#8", "Interval#9", "Interval#10",
                    "Interval#11", "Interval#12", "Interval#13", "Interval#14", "Interval#15", "Interval#16", "Interval#17", "Interval#18", "Interval#19", "Interval#20",
                    "Interval#21", "Interval#22", "Interval#23", "Interval#24"]

# Reading the data frame file from azure storage account
dat_df = spark.read.option("delimiter", "|").csv(uri+"/InputData/DailyMeterData.dat", header=True)

# Reading the CSV file from azure storage account
csvDataFile = spark.read.csv(uri+'/InputData/CustMeter.csv', header=True)


#This method is for joining the two files and converting it to long format to wide format 
def melt_csv(csvDataFile,dat_df,common_columns,QC_columns,Interval_columns):
        global joined_df
        dat_df_csv_datafile = csvDataFile.join(dat_df.drop("Meter Number"), on="Customer Account Number", how="inner")
        dataframe_pivot1 = dat_df_csv_datafile.unpivot(common_columns, QC_columns, "IntervalHour", "QCCode")
        dataframe_pivot2 = dat_df_csv_datafile.unpivot(common_columns, Interval_columns, "IntervalHour",
                                                       "IntervalValue")
        df1 = dataframe_pivot2.withColumn("IntervalHour", substring("IntervalHour", 10, 2).cast("int"))
        df2 = dataframe_pivot1.withColumn("IntervalHour", substring("IntervalHour", 4, 2).cast("int"))
        joined_df = df1.join(df2, on=['Customer Account Number', 'Meter Number', 'ServiceType', 'DT', 'Serial Number','Port', 'Channel', 'Conversion Factor', 'Data Type', 'Start Date', 'Start Time',
                                      "IntervalHour", ], how="inner")


melt_csv(csvDataFile,dat_df,common_columns,QC_columns,Interval_columns)


#This codes drops the duplicates and sorts the file as mentioned in the assignment(based on columns and in ascending order) 
no_duplicates=joined_df.dropDuplicates();
df_filtered = no_duplicates.filter((no_duplicates["Data Type"] != "Reverse Energy in Wh") & (no_duplicates["Data Type"] != "Net Energy in WH") & (no_duplicates["QCCode"] == 3))
df_filtered_sorted = df_filtered.orderBy(
    asc("Customer Account Number"),
    asc("Data Type"),
    asc("Meter Number"),
    asc("Start Date"),
    asc("IntervalHour")
)

def store_file(df_filtered_sorted,storage):
    #The below line of code stores the parquet file in two different files in the azure storage account
    df_filtered_sorted.write.mode('overwrite').parquet(storage+"Parquet")

    #The below line of code stores the parquet file in a single file as we are using repartition() 
    num_partitions = 1
    df_filtered_sorted = df_filtered_sorted.repartition(num_partitions)
    df_filtered_sorted.write.mode('overwrite').parquet(storage+"partition/Parquet")

    #The below line of code stores the parquet file in a single file as we are using coalesce(1) mentioned in the tutorial video 
    df_filtered_sorted.coalesce(1).write.option('header', True).mode('overwrite').parquet(uri+"Output/coalesce/Parquet")

    #The below line of code stores the CVS file to azure account
    df_filtered_sorted.write.option('header', True).mode('overwrite').csv(uri+"Output/CSV")
 
   

store_file(df_filtered_sorted,storage)