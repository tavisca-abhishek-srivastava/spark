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
print((df.select("name","age").show()))
# or 
print((df.select(df.name,df.age).show()))

#or
from pyspark.sql.functions import col
print((df.select(col("roll"),col("name")).show()))

# to list all columns
print((df.select('*').show()))

# to list columns using df.columns
print(": \n {0} ".format((df.select(df.columns[0:3]).show())))

# filtering out columns by creating new DataFrame
df1 = df.select(df.columns[0:4])
print(df1.show())
