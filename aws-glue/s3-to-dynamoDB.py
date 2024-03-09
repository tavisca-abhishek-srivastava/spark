import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
# Script generated for node Amazon S3
AmazonS3_node1671617715902 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dynamodb-export-bnr/AWSDynamoDB/01709576156860-fd8a7a0c/data"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1671617715902",
)
AmazonS3_node1671617715902.printSchema()
AmazonS3_node1671617715902.show()
# Script generated for node Change Schema (Apply Mapping)
ChangeSchemaApplyMapping_node1671617944126 = ApplyMapping.apply(
    frame=AmazonS3_node1671617715902,
    mappings=[
        
        (
            "Item.pk .S",
            "string",
            "pk",
            "string",
        ),
        (
            "Item.sk .S",
            "string",
            "sk",
            "string",
        )
    ],
    transformation_ctx="ChangeSchemaApplyMapping_node1671617944126",
)
ChangeSchemaApplyMapping_node1671617944126.printSchema()
ChangeSchemaApplyMapping_node1671617944126.show()
glueContext.write_dynamic_frame_from_options(
    frame=ChangeSchemaApplyMapping_node1671617944126,
    connection_type="dynamodb",
    connection_options={
        "dynamodb.output.tableName": "DynamoDB-GSI-LSI-dest",
        "dynamodb.throughput.write.percent": 0.5
    })
job.commit()