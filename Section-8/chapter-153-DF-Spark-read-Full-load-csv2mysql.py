import findspark
findspark.init()
from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn
from pyspark.sql.functions import col , when    
import os
import sys , math , random
os.system('cls||clear')

#ss = SparkSession.builder.appName("cdc").getOrCreate()
ss = SparkSession.builder.config("spark.driver.extraClassPath", "/tmp/postgresql-42.7.3.jar").appName("First ETL").getOrCreate()
csvname = "./LOAD00000001.csv"
#below option is for Provided schmea
df = ss.read.options( delemeter=',',inferSchema='True').csv(csvname)
os.system('cls||clear')
# used or '|' operator 

#print(df1.show())
driver = "org.postgresql.Driver"
url = "jdbc:postgresql://serverless-db.cluster-cfihahod2vsa.us-east-1.rds.amazonaws.com/students"
user = "postgres"
password = "welcome1234"
table = "students.pyspark.pyspark_servers"    ### make sure that this table doesn't exist 
df4.write.format("jdbc").option("driver",driver).option("url", url).option("user",user).option("password", password).option("dbtable", table).option("mode","append")