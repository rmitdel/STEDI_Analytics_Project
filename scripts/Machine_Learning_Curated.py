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

# Script generated for node Step_Trainer_Trusted
Step_Trainer_Trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://testing-bucket-rm1/step_trainer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="Step_Trainer_Trusted_node1",
)

# Script generated for node Accelerometer_Trusted
Accelerometer_Trusted_node1686497835297 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://testing-bucket-rm1/accelerometer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="Accelerometer_Trusted_node1686497835297",
)

# Script generated for node Join
Join_node1686497886577 = Join.apply(
    frame1=Step_Trainer_Trusted_node1,
    frame2=Accelerometer_Trusted_node1686497835297,
    keys1=["sensorReadingTime"],
    keys2=["timeStamp"],
    transformation_ctx="Join_node1686497886577",
)

# Script generated for node Drop Fields
DropFields_node1686498986897 = DropFields.apply(
    frame=Join_node1686497886577,
    paths=["timeStamp", "user"],
    transformation_ctx="DropFields_node1686498986897",
)

# Script generated for node Amazon S3
AmazonS3_node1686499022860 = glueContext.getSink(
    path="s3://testing-bucket-rm1/Machine Learning/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1686499022860",
)
AmazonS3_node1686499022860.setCatalogInfo(
    catalogDatabase="stedidatabase1", catalogTableName="machine_learning_curated"
)
AmazonS3_node1686499022860.setFormat("json")
AmazonS3_node1686499022860.writeFrame(DropFields_node1686498986897)
job.commit()
