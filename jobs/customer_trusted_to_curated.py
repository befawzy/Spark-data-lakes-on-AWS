import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer trusted
accelerometertrusted_node1705245575283 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-bfawzy/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometertrusted_node1705245575283",
)

# Script generated for node customer trusted
customertrusted_node1705245614672 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-bfawzy/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customertrusted_node1705245614672",
)

# Script generated for node get unique users from accelerometer trusted
getuniqueusersfromaccelerometertrusted_node1705246393154 = DynamicFrame.fromDF(
    accelerometertrusted_node1705245575283.toDF().dropDuplicates(["user"]),
    glueContext,
    "getuniqueusersfromaccelerometertrusted_node1705246393154",
)

# Script generated for node Join
Join_node1705245867099 = Join.apply(
    frame1=customertrusted_node1705245614672,
    frame2=getuniqueusersfromaccelerometertrusted_node1705246393154,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1705245867099",
)

# Script generated for node drop accelerometer fields
dropaccelerometerfields_node1705245925000 = DropFields.apply(
    frame=Join_node1705245867099,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="dropaccelerometerfields_node1705245925000",
)

# Script generated for node customer_curated
customer_curated_node1705245968058 = glueContext.write_dynamic_frame.from_options(
    frame=dropaccelerometerfields_node1705245925000,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-bfawzy/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="customer_curated_node1705245968058",
)

job.commit()
