#### flow 
## export data from dynamodb to s3 using dynamodb export
## using glue crawler import data from s3 into glue data catalog table
## using glue spark read data from glue data catalog table

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
 ## data in glue catalog table came from s3 using glue crawler
ReadDF = glueContext.create_dynamic_frame.from_catalog(
    database = "s3-iac",   #### glue catalog database
    table_name = "data",   #### glue catalog table
    transformation_ctx = "ReadDF",
)
df = ReadDF.toDF()
df.printSchema()
df.show(truncate=False)
df.show(10,truncate=False)
df.count()     # count will be same if we use athena query to get the count of data catlog table data