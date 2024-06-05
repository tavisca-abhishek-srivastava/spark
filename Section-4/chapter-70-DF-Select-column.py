from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession
import os
import sys , math , random
from pyspark.conf import SparkConf
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||clear')

conf = SparkConf()

#### run command sc.getConf().getAll()    ### note it is "sc" -> sparkContext  https://www.edureka.co/community/5268/how-to-change-the-spark-session-configuration-in-pyspark
conf.setAll(
        [
            ("spark.master","local[*]"),
            ("spark.driver.host", "ip-10-238-45-248.ec2.internal"),
            ("spark.submit.deployMode", "client"),    
            ("spark.app.name", "First DF App"),
        ] 
    )

SparkSchema = StructType([
                            StructField("age", IntegerType(),True),
                            StructField("gender", StringType(),True),
                            StructField("name", StringType(),True),
                            StructField("course", StringType(),True),
                            StructField("roll", StringType(),True),
                            StructField("marks", IntegerType(),True),
                            StructField("email", StringType(),True),
                        ])



### stop spark context
# spark.sparkContext.stop()
# ss = SparkSession.builder.appName("First DF App").getOrCreate()
ss = SparkSession.builder.config(conf=conf).getOrCreate()
# ss.sparkContext
# ss.sparkContext.stop()
# ss.sparkContext
# ss = SparkSession.builder.config(conf=conf).getOrCreate()
# ss.sparkContext
print(ss.sparkContext.getConf().getAll())

ss.sparkContext.setLogLevel('WARN')
#below option is for Provided schmea
df = ss.read.options( header='True', delemeter=',').schema(SparkSchema).csv("./StudentData.csv")
# print((df.select("name","age").show()))
print(df.select("name","age").show())
# or 
# print((df.select(df.name,df.age).show()))
print(df.select(df.name,df.age).show())
#or
from pyspark.sql.functions import col
#print((df.select(col("roll"),col("name")).show()))
print(df.select(col("roll"),col("name")).show())

# to list all columns
# print((df.select('*').show()))
print(df.select("*").show())

# to list columns using df.columns
# print("All columns : \n {0} ".format((df.columns)))
print("All columns : \n {0} ".format(df.columns))

# filtering out columns by creating new DataFrame
#df1 = df.select(df.columns[0:4])
df1 = df.select(df.columns[0:4])

print(df1.show())
