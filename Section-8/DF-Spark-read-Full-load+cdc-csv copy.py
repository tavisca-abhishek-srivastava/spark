import findspark
findspark.init()
from pyspark.sql.types import StructType,StructField , IntegerType,StringType
from pyspark.sql import SparkSession , functions as fn
from pyspark.sql.functions import col , when    
import os
import sys , math , random
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
#os.environ['HADOOP_HOME'] = "C:\\Hadoop"

os.system('cls||system')

ss = SparkSession.builder.appName("cdc").getOrCreate()

full_load_csvname = "C:\\Users\\abhishek.srivastava\\vscode\work\\Spark\\Section-8\\finaloutput"
cdc_csv = "C:\\Users\\abhishek.srivastava\\vscode\work\\Spark\Section-8\\20230322-042944267.csv"
#below option is for Provided schmea
cdc_df = ss.read.options( delemeter=',',inferSchema='True').csv(cdc_csv)
cdc_df2 = cdc_df.withColumnRenamed("_c1" , "id").withColumnRenamed("_c2" , "FullName").withColumnRenamed("_c3" , "City").withColumnRenamed("_c0" , "Action")

final_df = ss.read.csv(full_load_csvname)
final_df = final_df.withColumnRenamed("_c0" , "id").withColumnRenamed("_c1" , "FullName").withColumnRenamed("_c2" , "City")

for row in cdc_df2.collect():
    #print(row)
    if(row["Action"] == 'U'):
        #print("It is update")
        pass
    if(row["Action"] == 'I'):
        
        cdc_row = [list(row[1:])]
        column_header = ["id", "FullName" , "City"]
        cdc_insert_df = ss.createDataFrame(cdc_row , column_header) # temperory DataFrame with just 1 record
        final_df = final_df.union(cdc_insert_df)
        print(final_df.show())

    if(row["Action"] == 'D'):
        #print("It is Delete")
        final_df = final_df.filter( final_df.id != row["id"])
        


#print(final_df.show())

