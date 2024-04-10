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
df = ss.read.options( header='True', delemeter=',').csv("./StudentData.csv")
print(df.printSchema())

# Here changed the type of roll to string in Dataframe
# df22 = df.withColumn("roll", col("roll").cast("Integer"))
df22 = df.withColumn("roll",(col("roll")+30).cast(IntegerType()))
print(df22.printSchema())

### adding new column with name Updated marks" with value of marks column + 20
### Increment value of a particular column without iterating entire dataset and breaking the row in list
df1 = df22.withColumn("Updated marks" ,(col("marks") + 20).cast(IntegerType()))  # without cast type of "Updated marks will be double"
print(df1.printSchema())
# Add/Create a new Column on the fly in DataFrame
df2 = df1.withColumn("Min Passing mark",  fn.lit(33))   ### lit is literal which allows to put any fixed value in a column for all rows
df3 = df2.withColumn("Country",  fn.lit("India"))
print(df3.show())
#print(df1.printSchema())

### or 
df21 = df1.withColumn("Min Passing mark",  fn.lit(33)).withColumn("Country",  fn.lit("Japan"))
print(df21.show())