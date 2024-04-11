from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn 
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col , udf
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||system')

# def get_total_salary(salary , bonus):
#     return salary + bonus


ss = SparkSession.builder.appName("First DF App").getOrCreate()
ss.sparkContext.setLogLevel('WARN')
#below option is for Provided schmea
df = ss.read.options( header='True', delemeter=',',inferSchema='True').csv("./OfficeData.csv")
os.system('cls||system')

# TotalSalaryUDF = udf(lambda x , y : get_total_salary(x , y),IntegerType())

# df1 = df.withColumn("Total_Salary",TotalSalaryUDF(df.salary , df.bonus))
# print(df1.show())

def get_total_salary(salary , bonus):
    return salary + bonus

computer_total_salary_DUF = udf(lambda x,y : get_total_salary(x,y), IntegerType())

df1 = df.withColumn("Total_Salary", computer_total_salary_DUF(df.salary,df.bonus))
print(df1.show())

#### or 
@udf(returnType=IntegerType())
def get_total_salary(salary , bonus):
    return salary + bonus

df1 = df.withColumn("Total_Salary", get_total_salary(df.salary,df.bonus))
print(df1.show())