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

# Script generated for node Accelerator_Trusted
Accelerator_Trusted_node1686496004486 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://testing-bucket-rm1/accelerometer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="Accelerator_Trusted_node1686496004486",
)

# Script generated for node Customer_Trusted
Customer_Trusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://testing-bucket-rm1/customer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="Customer_Trusted_node1",
)

# Script generated for node Join
Join_node1686496155428 = Join.apply(
    frame1=Customer_Trusted_node1,
    frame2=Accelerator_Trusted_node1686496004486,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1686496155428",
)

# Script generated for node Drop Fields
DropFields_node1686496223811 = DropFields.apply(
    frame=Join_node1686496155428,
    paths=["z", "timeStamp", "user", "y", "x"],
    transformation_ctx="DropFields_node1686496223811",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1686496240023 = DynamicFrame.fromDF(
    DropFields_node1686496223811.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1686496240023",
)

# Script generated for node Amazon S3
AmazonS3_node1686496259509 = glueContext.getSink(
    path="s3://testing-bucket-rm1/Customer_Curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1686496259509",
)
AmazonS3_node1686496259509.setCatalogInfo(
    catalogDatabase="stedidatabase1", catalogTableName="customer_curated"
)
AmazonS3_node1686496259509.setFormat("json")
AmazonS3_node1686496259509.writeFrame(DropDuplicates_node1686496240023)
job.commit()
