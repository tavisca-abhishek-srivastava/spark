from pyspark.sql.types import  StructType , StructField, IntegerType , StringType
from pyspark.sql import SparkSession
import os
import sys , math , random
# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
# os.system('cls||clear')

##### structType represents a row and structFiled specify a column
# customSparkSchema = StructType([
#                             StructField("age", IntegerType(),True),
#                             StructField("gender", StringType(),True),
#                             StructField("name", StringType(),True),
#                             StructField("course", StringType(),True),
#                             StructField("roll", StringType(),True),
#                             StructField("marks", IntegerType(),True),
#                             StructField("email", StringType(),True),
#                         ])


customSparkSchema1 = StructType([
    StructField("age",IntegerType(),True),
    StructField("gender",StringType(),True),
    StructField("name", StringType(),True),
    StructField("course", StringType(),True),
    StructField("roll", StringType(),True),
    StructField("marks", IntegerType(),True),
    StructField("email", StringType(),True),
])



ss = SparkSession.builder.appName("my provided schema").getOrCreate()
ss.sparkContext.setLogLevel('WARN')
#####below option is for Provided schmea
df = ss.read.options( header='True', delemeter=',').schema(customSparkSchema1).csv("./StudentData.csv")

print((df.show(truncate=False)))
print((df.printSchema()))