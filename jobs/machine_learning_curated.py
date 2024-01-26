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

# Script generated for node step trainer trusted
steptrainertrusted_node1705249914709 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-bfawzy/step_trainer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="steptrainertrusted_node1705249914709",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1705249947143 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-lake-house-bfawzy/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometertrusted_node1705249947143",
)

# Script generated for node Join
Join_node1705249943770 = Join.apply(
    frame1=steptrainertrusted_node1705249914709,
    frame2=accelerometertrusted_node1705249947143,
    keys1=["sensorReadingTime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1705249943770",
)

# Script generated for node machine learning curated
machinelearningcurated_node1705250062120 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1705249943770,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-lake-house-bfawzy/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="machinelearningcurated_node1705250062120",
)

job.commit()
