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

# Script generated for node Data Catalog table
DataCatalogtable_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="landing",
    table_name="covid_19_clean_complete",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("province_state", "string", "province_state", "string"),
        ("country_region", "string", "country_region", "string"),
        ("lat", "string", "lat", "string"),
        ("long", "string", "long", "string"),
        ("date", "string", "date", "string"),
        ("confirmed", "string", "confirmed", "string"),
        ("deaths", "string", "deaths", "string"),
        ("recovered", "string", "recovered", "string"),
        ("active", "string", "active", "string"),
        ("who_region", "string", "who_region", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Data Catalog table
DataCatalogtable_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="bronze",
    table_name="covid_19_clean_complete",
    transformation_ctx="DataCatalogtable_node3",
)

job.commit()
