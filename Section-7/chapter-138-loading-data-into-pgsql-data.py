from pyspark.sql import SparkSession , functions as f
from pyspark.sql.functions import lit , col , explode

##### wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
#### copy  "postgresql-42.7.3.jar" to "spark-3.4.3-bin-hadoop3/jars" folder
ss = SparkSession.builder.config("spark.driver.extraClassPath", "/tmp/postgresql-42.7.3.jar").appName("First ETL").getOrCreate()
#ss = SparkSession.builder.config("spark.jars.packages", "/tmp/postgresql-42.7.3.jar").appName("First ETL").getOrCreate()
ss.sparkContext.setLogLevel("WARN")
df = ss.read.text("./abc.txt")
df2 = df.withColumn("splitted words" , f.split("value", " "))
df3 = df2.withColumn("words",explode("splitted words"))
df4 = df3.groupBy("words").count()

driver = "org.postgresql.Driver"
url = "jdbc:postgresql://serverless-db.cluster-cfihahod2vsa.us-east-1.rds.amazonaws.com/students"
user = "postgres"
password = "welcome1234"
table = "students.pyspark.pyspark_injest1"    ### make sure that this table doesn't exist 
df4.write.format("jdbc").option("driver",driver).option("url", url).option("user",user).option("password", password).option("dbtable", table).option("mode","append").save()

print(df.show())

