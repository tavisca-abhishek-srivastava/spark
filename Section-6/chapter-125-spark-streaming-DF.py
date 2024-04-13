from pyspark.sql import SparkSession
spark_session = SparkSession.builder.appName("Streaming using DF").getOrCreate()
spark_session.sparkContext.setLogLevel("WARN")
sdf = spark_session.readStream.json("./sparkstreaming/output")
sdf.writeStream.format("console").outputMode("complete").start()
