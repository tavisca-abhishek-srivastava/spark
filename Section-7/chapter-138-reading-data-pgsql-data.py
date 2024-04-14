from pyspark.sql import SparkSession , functions as f
from pyspark.sql.functions import lit , col , explode
##### wget https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
ss = SparkSession.builder.config("spark.driver.extraClassPath", "/tmp/postgresql-42.7.3.jar").appName("First ETL").getOrCreate()
#ss = SparkSession.builder.config("spark.jars.packages", "/tmp/postgresql-42.7.3.jar").appName("First ETL").getOrCreate()
ss.sparkContext.setLogLevel("WARN")
driver = "org.postgresql.Driver"
url = "jdbc:postgresql://serverless-db.cluster-cfihahod2vsa.us-east-1.rds.amazonaws.com/students"
user = "postgres"
password = "welcome1234"
table = "students.pyspark.pyspark_injest"
df = ss.read.format("jdbc").option("driver",driver).option("url", url).option("user",user).option("password", password).option("dbtable", table).option("mode","append").load()

print(df.show())

