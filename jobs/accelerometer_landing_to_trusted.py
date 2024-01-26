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

# Script generated for node accelerometer landing
accelerometerlanding_node1705248483324 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-bfawzy/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="accelerometerlanding_node1705248483324",
)

# Script generated for node customer trusted
customertrusted_node1705248538978 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-bfawzy/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1705248538978",
)

# Script generated for node Join
Join_node1705248535501 = Join.apply(
    frame1=accelerometerlanding_node1705248483324,
    frame2=customertrusted_node1705248538978,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1705248535501",
)

# Script generated for node get accelerometer relevant fields
getaccelerometerrelevantfields_node1705248656231 = DropFields.apply(
    frame=Join_node1705248535501,
    paths=[
        "birthDay",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "registrationDate",
        "serialNumber",
        "shareWithFriendsAsOfDate",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
    ],
    transformation_ctx="getaccelerometerrelevantfields_node1705248656231",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1705248608611 = glueContext.write_dynamic_frame.from_options(
    frame=getaccelerometerrelevantfields_node1705248656231,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-bfawzy/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="accelerometertrusted_node1705248608611",
)

job.commit()
