from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn
from pyspark.sql.functions import col
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

ss = SparkSession.builder.appName("First DF App").getOrCreate()
### below option is for Provided schmea
df = ss.read.options( header='True', delemeter=',',inferSchema='True').csv("./OfficeData.csv")
os.system('cls||system')
### used or '|' operator 
#### here data is sorted by bonus
df4 = df.sort(df.bonus.asc())
os.system('cls||system')
print(df4.show())

### here data is sorted by age in ascending and then by salary in ascending order
df5 = df.sort(df.age.desc(), df.salary.asc())
print(df5.show())

### here data is sorted by age in descending order, then by bonus in descending order and then by salary in ascending order
df6 = df.sort(df.age.desc(),df.bonus.desc(), df.salary.asc())
print(df6.show())
