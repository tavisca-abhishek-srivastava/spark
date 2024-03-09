from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn 
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col , udf
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

def get_total_salary(salary , bonus):
    return salary + bonus


ss = SparkSession.builder.appName("First DF App").getOrCreate()
#below option is for Provided schmea
df = ss.read.options( header='True', delemeter=',',inferSchema='True').csv("C:\\Users\\abhishek.srivastava\\vscode\work\\Spark\\Setion-4\\OfficeData.csv")
os.system('cls||system')

TotalSalaryUDF = udf(lambda x , y : get_total_salary(x , y),IntegerType())

df1 = df.withColumn("Total_Salary",TotalSalaryUDF(df.salary , df.bonus))
print(df1.show())
