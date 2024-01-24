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

# Script generated for node step trainer landing
steptrainerlanding_node1705245575283 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-bfawzy/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="steptrainerlanding_node1705245575283",
)

# Script generated for node customer curated
customercurated_node1705245614672 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-bfawzy/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="customercurated_node1705245614672",
)

# Script generated for node Join
Join_node1705245867099 = Join.apply(
    frame1=customercurated_node1705245614672,
    frame2=steptrainerlanding_node1705245575283,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1705245867099",
)

# Script generated for node get step trainer relevant fields
getsteptrainerrelevantfields_node1705247471016 = DropFields.apply(
    frame=Join_node1705245867099,
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
    transformation_ctx="getsteptrainerrelevantfields_node1705247471016",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1705245968058 = glueContext.write_dynamic_frame.from_options(
    frame=getsteptrainerrelevantfields_node1705247471016,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-bfawzy/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="steptrainertrusted_node1705245968058",
)

job.commit()
