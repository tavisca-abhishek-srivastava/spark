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
ss.sparkContext.setLogLevel('WARN')
csvname = "./LOAD00000001.csv"
#below option is for Provided schmea
df = ss.read.options( delemeter=',',inferSchema='True').csv(csvname)
# os.system('cls||clear')
# used or '|' operator 

print(df.show())
driver = "org.postgresql.Driver"
url = "jdbc:postgresql://serverless-db.cluster-cfihahod2vsa.us-east-1.rds.amazonaws.com/students"
user = "postgres"
password = "welcome1234"
table = "students.pyspark.pyspark_servers_trimmed"    ### make sure that this table doesn't exist 
df1 = df.withColumnRenamed("_c0" , "id").withColumnRenamed("_c1" , "instance_id").withColumnRenamed("_c2" , "instance_type").withColumnRenamed("_c3" , "state") \
        .withColumnRenamed("_c4" , "name").withColumnRenamed("_c5" , "availability_zone").withColumnRenamed("_c6" , "create_time").withColumnRenamed("_c7" , "platform").withColumnRenamed("_c8" , "image_id") \
        .withColumnRenamed("_c9" , "key_name").withColumnRenamed("_c10" , "private_ip_address").withColumnRenamed("_c11" , "role_name").withColumnRenamed("_c12" , "detailed_monitoring").withColumnRenamed("_c13" , "subnet_resource_id") \
        .withColumnRenamed("_c14" , "account")

df1.write.format("jdbc").option("driver",driver).option("url", url).option("user",user).option("password", password).option("dbtable", table).option("mode","append").save()