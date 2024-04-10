from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['HADOOP_HOME'] = "C:\\Windows\\System32"
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

# for RDD we need SparkConf and SparkContext
conf = SparkConf().setAppName("RDD2DF")
sc = SparkContext(master='local').getOrCreate(conf=conf)

rdd = sc.textFile("./StudentData.csv")
header = rdd.first()

rdd1 = rdd.filter(lambda x: x != header)
rdd2 = rdd1.map(lambda x : x.split(','))
# to have same data type as defined in schema, we have to convert the data type
rdd3 = rdd2.map(lambda x: [int(x[0]),x[1],x[2],x[3],x[4],int(x[5]),x[6] ])

# For DF we need SparkSession
spark_session = SparkSession.builder.appName("RDD2DF").getOrCreate()
df = spark_session.createDataFrame(rdd3,schema=SparkSchema)
os.system('cls||system')
print(df.show())
print(df.printSchema())