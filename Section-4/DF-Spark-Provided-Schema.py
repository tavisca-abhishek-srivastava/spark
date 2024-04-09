from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

SparkSchema = StructType([
                            StructField("age", IntegerType(),True),
                            StructField("gender", StringType(),True),
                            StructField("name", StringType(),True),
                            StructField("course", StringType(),True),
                            StructField("roll", StringType(),True),
                            StructField("marks", IntegerType(),True),
                            StructField("email", StringType(),True),
                        ])

ss = SparkSession.builder.appName("First DF App").getOrCreate()
#below option is for Provided schmea
df = ss.read.options( header='True', delemeter=',').schema(SparkSchema).csv("C:\\Users\\abhishek.srivastava\\vscode\work\\Spark\\Setion-4\\StudentData.csv")
print((df.show()))
print((df.printSchema()))