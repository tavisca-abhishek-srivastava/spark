import findspark
findspark.init()
from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn
from pyspark.sql.functions import col , when    
import os
import sys , math , random

# os.system('cls||clear')

ss = SparkSession.builder.appName("cdc").getOrCreate()
ss.sparkContext.setLogLevel('WARN')

full_load_csvname = "./LOAD00000001.csv"
cdc_csv = "./20230322-042944267.csv"
final_output_csvname = "./finaloutput"

######################## below option is for Provided schmea

df = ss.read.options( delemeter=',',inferSchema='True').csv(full_load_csvname)
df1 = df.withColumnRenamed("_c0" , "id").withColumnRenamed("_c1" , "FullName").withColumnRenamed("_c2" , "City")
# os.system('cls||clear')
# used or '|' operator 
df1.write.mode("overwrite").csv(final_output_csvname)
#print(df1.show(df1.count(), False))

#########################  

#below option is for Provided schmea
cdc_df = ss.read.options( delemeter=',',inferSchema='True').csv(cdc_csv)
cdc_df2 = cdc_df.withColumnRenamed("_c1" , "id").withColumnRenamed("_c2" , "FullName").withColumnRenamed("_c3" , "City").withColumnRenamed("_c0" , "Action")

############################ read final_output_csvname
final_output_df = ss.read.options( delemeter=',',inferSchema='True').csv(final_output_csvname)
final_output_df = final_output_df.withColumnRenamed("_c0" , "id").withColumnRenamed("_c1" , "FullName").withColumnRenamed("_c2" , "City")
#print(final_output_df.show(df1.count(), False))
#######################################
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
        


print(final_output_df.show(final_output_df.count(), False))

