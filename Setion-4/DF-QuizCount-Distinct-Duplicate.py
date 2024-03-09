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
df = ss.read.options( header='True', delemeter=',',inferSchema='True').csv("C:\\Users\\abhishek.srivastava\\vscode\work\\Spark\\Setion-4\\StudentData.csv")
os.system('cls||system')
# used or '|' operator 
# here combination of gender and course will be considered for uniqueness
df4 = df.dropDuplicates(["age","gender", "course"])
print(df4.show())
print(df4.count())



