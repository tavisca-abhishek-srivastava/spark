from pyspark import SparkConf , SparkContext

conf = SparkConf().setAppName("file copy")

sc = SparkContext(master='local').getOrCreate(conf=conf)

rdd = sc.textFile("c:\\tmp\\abc.txt")
print(rdd.collect())

