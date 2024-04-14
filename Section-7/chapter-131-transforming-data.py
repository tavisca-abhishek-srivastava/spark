from pyspark.sql import SparkSession , functions as f
from pyspark.sql.functions import lit , col , explode
ss = SparkSession.builder.appName("First ETL").getOrCreate()
ss.sparkContext.setLogLevel("WARN")
df = ss.read.text("./abc.txt")
#print(df.show())

##### transform row which is string into row with list of strings by splitting

df2 = df.withColumn("splitted words" , f.split("value", " "))
df3 = df2.withColumn("words",explode("splitted words"))
df4 = df3.groupBy("words").count()




print(df4.show(truncate=False))


