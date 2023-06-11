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

# Script generated for node Amazon S3
AmazonS3_node1686492449397 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://testing-bucket-rm1/accelerometer/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1686492449397",
)

# Script generated for node Amazon S3
AmazonS3_node1686494025435 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://testing-bucket-rm1/customer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1686494025435",
)

# Script generated for node Join
Join_node1686492540928 = Join.apply(
    frame1=AmazonS3_node1686492449397,
    frame2=AmazonS3_node1686494025435,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1686492540928",
)

# Script generated for node Drop Fields
DropFields_node1686492690271 = DropFields.apply(
    frame=Join_node1686492540928,
    paths=[
        "email",
        "phone",
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1686492690271",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1686492711130 = DynamicFrame.fromDF(
    DropFields_node1686492690271.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1686492711130",
)

# Script generated for node Amazon S3
AmazonS3_node1686492737051 = glueContext.getSink(
    path="s3://testing-bucket-rm1/accelerometer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1686492737051",
)
AmazonS3_node1686492737051.setCatalogInfo(
    catalogDatabase="stedidatabase1", catalogTableName="accelerometer_trusted"
)
AmazonS3_node1686492737051.setFormat("json")
AmazonS3_node1686492737051.writeFrame(DropDuplicates_node1686492711130)
job.commit()
