from pyspark.sql import SparkSession

ss = SparkSession.builder.appName("First ETL").getOrCreate()

df = ss.read.text("./abc.txt")

print(df.show())

