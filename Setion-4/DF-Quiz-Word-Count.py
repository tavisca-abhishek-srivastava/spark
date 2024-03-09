from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn
from pyspark.sql.functions import col
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

ss = SparkSession.builder.appName("First DF App").getOrCreate()
#below option is for Provided schmea
df = ss.read.text("C:\\Users\\abhishek.srivastava\\vscode\work\\Spark\\Setion-4\\WordData.txt")
os.system('cls||system')
print(df.count())
