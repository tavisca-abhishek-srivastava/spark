from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn 
from pyspark.sql.types import IntegerType , FloatType , DoubleType
from pyspark.sql.functions import col , udf
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

@udf(returnType=DoubleType())
def get_total_salary(salary , bonus,state):
    if state == 'NY':
        return salary *1.1 + bonus * 1.05

    if state == 'CA':
        return salary *1.12 + bonus * 1.03

ss = SparkSession.builder.appName("First DF App").getOrCreate()
#below option is for Provided schmea
df = ss.read.options( header='True', delemeter=',',inferSchema='True').csv("./OfficeData.csv")
os.system('cls||clear')

# TotalSalaryUDF = udf(lambda x , y, z : get_total_salary(x , y, z),DoubleType())

df1 = df.withColumn("Total_Salary",get_total_salary(df.salary , df.bonus , df.state))
print(df1.show())

print(df1.show())