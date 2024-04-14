from pyspark.sql import SparkSession , functions as f
from pyspark.sql.functions import lit , col , explode
ss = SparkSession.builder.config("spark.driver.extraClassPath", "/tmp/postgresql-42.7.3.jar").appName("First ETL").config("spark.driver.extraClassPath", "/path/to/postgresql-<version>.jar").getOrCreate()
ss.sparkContext.setLogLevel("WARN")
driver = "org.postgresql.Driver"
url = "jdbc:postgresql://serverless-db-1-instance-1.cfihahod2vsa.us-east-1.rds.amazonaws.com:5432/>"
user = "postgres"
password = "welcome1234"
table = "students.pyspark.pyspark-injest"
df = ss.read.format("JDBC").option("driver",driver).option("url", url).option("user",user).option("password", password).option("dbtable", "db.table_name").option("mode","append").load()



