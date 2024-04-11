from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn
from pyspark.sql.functions import col
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

ss = SparkSession.builder.appName("First DF App").getOrCreate()
ss.sparkContext.setLogLevel('WARN')
#below option is for Provided schmea
df = ss.read.options( header='True', delemeter=',',inferSchema='True').csv("./StudentData.csv")
os.system('cls||system')
# used or '|' operator 
# here data is sorted by age
df4 = df.sort(["age"])
os.system('cls||system')
print(df4.show())
print(df4.count())

# here data is orderby age which is same as sort by age
df5 = df.orderBy(["age"])
print(df5.show())
print(df5.count())

# descending order
df6 = df.sort(df.marks.asc(),df.age.desc())
print(df6.show())
print(df6.count())
