from pyspark.streaming import StreamingContext
from pyspark.sql.streaming import DataStreamReader
from pyspark import SparkConf , SparkContext

sconf = SparkConf().setAppName("first Streaming App")
scontext = SparkContext(master='local').getOrCreate(conf=sconf)

ssc = StreamingContext(scontext,1)

rdd = ssc.textFileStream("./sparkstreaming/input")

rdd.pprint()

rdd.saveAsTextFiles("./sparkstreaming/output")


rdd1 = rdd.map(lambda x : (x,1))
rdd2 = rdd1.reduceByKey(lambda x ,y : x+y)
rdd2.pprint()

ssc.start()
ssc.awaitTermination(100000)

