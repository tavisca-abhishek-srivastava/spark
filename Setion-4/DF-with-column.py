from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn
from pyspark.sql.functions import col
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

ss = SparkSession.builder.appName("First DF App").getOrCreate()
#below option is for Provided schmea
df = ss.read.options( header='True', delemeter=',').csv("C:\\Users\\abhishek.srivastava\\vscode\work\\Spark\\Setion-4\\StudentData.csv")
print(df.printSchema())

# Here changed the type of roll to string in Dataframe
df22 = df.withColumn("roll", col("roll").cast("Integer"))
print(df22.printSchema())

#Increment value of a particular column without iterating entire dataset and breaking the row in list
df1 = df22.withColumn("Updated marks" ,(col("marks") + 20))

# Add/Create a new Column on the fly in DataFrame
df2 = df1.withColumn("Min Passing mark",  fn.lit(33))
df3 = df2.withColumn("Country",  fn.lit("India"))
print(df3.show())
#print(df1.printSchema())
