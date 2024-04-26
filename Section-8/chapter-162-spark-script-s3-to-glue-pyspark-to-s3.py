import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import when    


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['s3_target_path_key','s3_target_path_bucket'])
bucket = args['s3_target_path_bucket']
filename = args['s3_target_path_key']

ss = SparkSession.builder.appName("cdc").getOrCreate()
ss.sparkContext.setLogLevel('WARN')

inputfilepath = f"s3a://{bucket}/{filename}"
finalfilepath = f"s3a://"

if "LOAD" in filename:
    df = ss.read.options( delemeter=',',inferSchema='True').csv(inputfilepath)
    df1 = df.withColumnRenamed("_c0" , "id").withColumnRenamed("_c1" , "FullName").withColumnRenamed("_c2" , "City")
    df1.write.mode("overwrite").csv(finalfilepath) 
else:
    cdc_df = ss.read.options( delemeter=',',inferSchema='True').csv(inputfilepath)
    cdc_df2 = cdc_df.withColumnRenamed("_c0" , "Action").withColumnRenamed("_c1" , "id").withColumnRenamed("_c2" , "FullName").withColumnRenamed("_c3" , "City")
    ffdf = ss.read.options( delemeter=',',inferSchema='True').csv(finalfilepath)
    ffdf = ffdf.withColumnRenamed("_c0" , "id").withColumnRenamed("_c1" , "FullName").withColumnRenamed("_c2" , "City")