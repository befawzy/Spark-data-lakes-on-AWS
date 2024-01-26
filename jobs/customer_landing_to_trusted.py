import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Amazon S3
AmazonS3_node1705237465045 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-bfawzy/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1705237465045",
)

# Script generated for node AgreedToShareData
AgreedToShareData_node1705237617689 = Filter.apply(
    frame=AmazonS3_node1705237465045,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="AgreedToShareData_node1705237617689",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1705237786771 = glueContext.write_dynamic_frame.from_options(
    frame=AgreedToShareData_node1705237617689,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrustedZone_node1705237786771",
)

job.commit()
