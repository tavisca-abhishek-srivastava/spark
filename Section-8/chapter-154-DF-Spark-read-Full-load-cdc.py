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
#below option is for Provided schmea
df = ss.read.options( delemeter=',',inferSchema='True').csv(full_load_csvname = "./LOAD00000001.csv")
df1 = df.withColumnRenamed("_c0" , "id").withColumnRenamed("_c1" , "FullName").withColumnRenamed("_c2" , "City")
# os.system('cls||clear')
# used or '|' operator 
df1.write.mode("overwrite").csv("./finaloutput")

#########################

final_output__csvname = "./finaloutput"
cdc_csv = "./20230322-042944267.csv"
#below option is for Provided schmea
cdc_df = ss.read.options( delemeter=',',inferSchema='True').csv(cdc_csv)
cdc_df2 = cdc_df.withColumnRenamed("_c1" , "id").withColumnRenamed("_c2" , "FullName").withColumnRenamed("_c3" , "City").withColumnRenamed("_c0" , "Action")

#print(cdc_df2.collect())
for row in cdc_df2.collect() :
    print(type(row))
