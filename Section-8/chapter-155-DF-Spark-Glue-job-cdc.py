import findspark
findspark.init()
from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn
from pyspark.sql.functions import col , when    
import os
import sys , math , random
os.system('cls||clear')

ss = SparkSession.builder.appName("cdc").getOrCreate()

csvname = "./LOAD00000001.csv"
#below option is for Provided schmea
df = ss.read.options( delemeter=',',inferSchema='True').csv(csvname)
udf = df.withColumnRenamed("_c0" , "id").withColumnRenamed("_c1" , "FullName").withColumnRenamed("_c2" , "City")
print(udf.collect())