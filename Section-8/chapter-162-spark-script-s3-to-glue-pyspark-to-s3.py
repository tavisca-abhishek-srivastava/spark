import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import when    


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['s3_target_path_key','s3_target_path_bucket'])

bucket = args['s3_target_path_bucket']
filename = args['s3_target_path_key']

ss = SparkSession.builder.appName("cdc").getOrCreate()
ss.sparkContext.setLogLevel('WARN')
