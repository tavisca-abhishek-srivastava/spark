from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn
from pyspark.sql.functions import col 
from pyspark.sql.functions import max , min , sum , avg , mean,count

import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.system('cls||clear')

# ss = SparkSession.builder.appName("First DF App").getOrCreate()
# ###below option is for Provided schmea
# df = ss.read.options( header='True', delemeter=',',inferSchema='True').csv("C:\\Users\\abhishek.srivastava\\vscode\work\\Spark\\Setion-4\\StudentData.csv")
# os.system('cls||system')

# #### apart from columns in group by all other columns will be part of aggregate function which you want to display
# df1 = df.groupBy('gender').count()
# print(df1.show())
# ### filtering 

# df2 = df.filter(df.gender == 'Male')
# df3 = df2.groupBy("course","gender").agg(count('*').alias("Total Enrollment"))
# df4 = df3.filter(col("Total Enrollment") > 70)
# print(df4.show())

spark_session = SparkSession.builder.appName("groupBy").getOrCreate()
spark_session.sparkContext.setLogLevel('WARN')
## set logging level
spark_session.sparkContext.setLogLevel('WARN')
df = spark_session.read.options(delemeter=",",inferSchema="True",header="True").csv("./StudentData.csv")
df1 = df.groupBy("gender").sum("marks")
print(df1.show())

## how many male and female are enrolled
gender_wise_count_df = df.groupBy("gender").count()
print(gender_wise_count_df.show())

## how many male and female are enrolled for each course
course_wise_enrollment_df = df.groupBy("course").count()
print(course_wise_enrollment_df.show())

## sum of marks for each course
course_wise_total_marks_df = df.groupBy("course").sum("marks")
print(course_wise_total_marks_df.show())

## min marks male and female
gender_wise_min_marks_df = df.groupBy("gender").min("marks")
print(gender_wise_min_marks_df.show())


## max marks male and female
gender_wise_max_marks_df = df.groupBy("gender").max("marks")
print(gender_wise_max_marks_df.show())