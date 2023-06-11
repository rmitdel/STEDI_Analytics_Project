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

# Script generated for node customer_landing
customer_landing_node1686486527121 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://testing-bucket-rm1/Customer_Records/"]},
    transformation_ctx="customer_landing_node1686486527121",
)

# Script generated for node research_sharing
research_sharing_node1686486562875 = Filter.apply(
    frame=customer_landing_node1686486527121,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="research_sharing_node1686486562875",
)

# Script generated for node customer_trusted_data
customer_trusted_data_node1686486706255 = glueContext.getSink(
    path="s3://testing-bucket-rm1/customer_trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_data_node1686486706255",
)
customer_trusted_data_node1686486706255.setCatalogInfo(
    catalogDatabase="stedidatabase1", catalogTableName="customer_trusted"
)
customer_trusted_data_node1686486706255.setFormat("json")
customer_trusted_data_node1686486706255.writeFrame(research_sharing_node1686486562875)
job.commit()
