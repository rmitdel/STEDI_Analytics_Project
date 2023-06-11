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

# Script generated for node Step_trainer_landing
Step_trainer_landing_node1686496626070 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://testing-bucket-rm1/step_trainer/"],
        "recurse": True,
    },
    transformation_ctx="Step_trainer_landing_node1686496626070",
)

# Script generated for node Customer_Curated
Customer_Curated_node1686496653152 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://testing-bucket-rm1/Customer_Curated/"],
        "recurse": True,
    },
    transformation_ctx="Customer_Curated_node1686496653152",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1686496790365 = ApplyMapping.apply(
    frame=Customer_Curated_node1686496653152,
    mappings=[
        ("serialNumber", "string", "`(right) serialNumber`", "string"),
        ("birthDay", "string", "`(right) birthDay`", "string"),
        (
            "shareWithResearchAsOfDate",
            "long",
            "`(right) shareWithResearchAsOfDate`",
            "long",
        ),
        ("registrationDate", "long", "`(right) registrationDate`", "long"),
        ("customerName", "string", "`(right) customerName`", "string"),
        (
            "shareWithFriendsAsOfDate",
            "long",
            "`(right) shareWithFriendsAsOfDate`",
            "long",
        ),
        ("email", "string", "`(right) email`", "string"),
        ("lastUpdateDate", "long", "`(right) lastUpdateDate`", "long"),
        ("phone", "string", "`(right) phone`", "string"),
        (
            "shareWithPublicAsOfDate",
            "long",
            "`(right) shareWithPublicAsOfDate`",
            "long",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1686496790365",
)

# Script generated for node Join
Join_node1686496774012 = Join.apply(
    frame1=Step_trainer_landing_node1686496626070,
    frame2=RenamedkeysforJoin_node1686496790365,
    keys1=["serialNumber"],
    keys2=["`(right) serialNumber`"],
    transformation_ctx="Join_node1686496774012",
)

# Script generated for node Drop Fields
DropFields_node1686496844937 = DropFields.apply(
    frame=Join_node1686496774012,
    paths=[
        "`(right) serialNumber`",
        "`(right) birthDay`",
        "`(right) shareWithResearchAsOfDate`",
        "`(right) registrationDate`",
        "`(right) customerName`",
        "`(right) shareWithFriendsAsOfDate`",
        "`(right) email`",
        "`(right) lastUpdateDate`",
        "`(right) phone`",
        "`(right) shareWithPublicAsOfDate`",
    ],
    transformation_ctx="DropFields_node1686496844937",
)

# Script generated for node Step_Trainer_Trusted
Step_Trainer_Trusted_node1686496861561 = glueContext.getSink(
    path="s3://testing-bucket-rm1/step_trainer_trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="Step_Trainer_Trusted_node1686496861561",
)
Step_Trainer_Trusted_node1686496861561.setCatalogInfo(
    catalogDatabase="stedidatabase1", catalogTableName="step_trainer_trusted"
)
Step_Trainer_Trusted_node1686496861561.setFormat("json")
Step_Trainer_Trusted_node1686496861561.writeFrame(DropFields_node1686496844937)
job.commit()
