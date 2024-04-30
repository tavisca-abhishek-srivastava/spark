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
finalfilepathtemp = f"s3a://cdc-poc-iac-target/outputtemp"
finalfilepath = f"s3a://cdc-poc-iac-target/output"

if "LOAD" in filename:
    df = ss.read.options( delemeter=',',inferSchema='True').csv(inputfilepath)
    df1 = df.withColumnRenamed("_c0" , "id").withColumnRenamed("_c1" , "FullName").withColumnRenamed("_c2" , "City")
    df1.write.mode("overwrite").csv(finalfilepath) 
else:
    cdc_df = ss.read.options( delemeter=',',inferSchema='True').csv(inputfilepath)
    cdc_df2 = cdc_df.withColumnRenamed("_c0" , "Action").withColumnRenamed("_c1" , "id").withColumnRenamed("_c2" , "FullName").withColumnRenamed("_c3" , "City")
    final_output_df = ss.read.options( delemeter=',',inferSchema='True').csv(finalfilepath)
    final_output_df = final_output_df.withColumnRenamed("_c0" , "id").withColumnRenamed("_c1" , "FullName").withColumnRenamed("_c2" , "City")
    
    for row in cdc_df2.collect():
        #print(row)
        if(row["Action"] == 'U'):
            print("It is update")
            final_output_df = final_output_df.withColumn("FullName", when(final_output_df["id"] == row["id"], row["FullName"]).otherwise(final_output_df["FullName"]))
            final_output_df = final_output_df.withColumn("City", when(final_output_df["id"] == row["id"], row["City"]).otherwise(final_output_df["City"]))

        if(row["Action"] == 'I'):
            print("Inserting {0}".format(row))
            cdc_row = [list(row[1:])]
            column_header = ["id", "FullName" , "City"]
            cdc_insert_df = ss.createDataFrame(cdc_row , column_header) # temperory DataFrame with just 1 record
            final_output_df = final_output_df.union(cdc_insert_df)
            #print(final_output_df.show(final_output_df.count(), False))

        if(row["Action"] == 'D'):
            #print("It is Delete")
            #print("Deleting {0}".format(row))
            final_output_df = final_output_df.filter( final_output_df.id != row["id"])

    final_output_df.write.mode("overwrite").csv(finalfilepathtemp)
    final_output_df = ss.read.options( delemeter=',',inferSchema='True').csv(finalfilepathtemp)
    final_output_df.write.mode("overwrite").csv(finalfilepath)

    
    print(final_output_df.show(final_output_df.count(), False))

