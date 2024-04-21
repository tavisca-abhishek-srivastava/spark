import findspark
findspark.init()
from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn
from pyspark.sql.functions import col , when    
import os
import sys , math , random

# os.system('cls||clear')

ss = SparkSession.builder.appName("cdc").getOrCreate()

full_load_csvname = "./LOAD00000001.csv"
cdc_csv = "./20230322-042944267.csv"
final_output__csvname = "./finaloutput/finaloutput.csv"

######################## below option is for Provided schmea

df = ss.read.options( delemeter=',',inferSchema='True').csv(full_load_csvname = "./LOAD00000001.csv")
df1 = df.withColumnRenamed("_c0" , "id").withColumnRenamed("_c1" , "FullName").withColumnRenamed("_c2" , "City")
# os.system('cls||clear')
# used or '|' operator 
df1.write.mode("overwrite").csv("./finaloutput/finaloutput.csv")

#########################  

#below option is for Provided schmea
cdc_df = ss.read.options( delemeter=',',inferSchema='True').csv(cdc_csv)
cdc_df2 = cdc_df.withColumnRenamed("_c1" , "id").withColumnRenamed("_c2" , "FullName").withColumnRenamed("_c3" , "City").withColumnRenamed("_c0" , "Action")

############################ read final_output__csvname
final_output__df = ss.read.options( delemeter=',',inferSchema='True').csv(final_output__csvname)

#######################################
for row in cdc_df2.collect():
    #print(row)
    if(row["Action"] == 'U'):
        #print("It is update")
        pass
    if(row["Action"] == 'I'):
        print("Inserting {0}".format(row))
        cdc_row = [list(row[1:])]
        column_header = ["id", "FullName" , "City"]
        cdc_insert_df = ss.createDataFrame(cdc_row , column_header) # temperory DataFrame with just 1 record
        final_df = final_output__df.union(cdc_insert_df)
        print(final_output__df.show())

    if(row["Action"] == 'D'):
        #print("It is Delete")
        print("Deleting {0}".format(row))
        final_output__df = final_output__df.filter( final_df.id != row["id"])
        


#print(final_df.show())

