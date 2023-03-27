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
    table_name="usa_county_wise",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("uid", "string", "uid", "string"),
        ("iso2", "string", "iso2", "string"),
        ("iso3", "string", "iso3", "string"),
        ("code3", "string", "code3", "string"),
        ("fips", "string", "fips", "string"),
        ("admin2", "string", "admin2", "string"),
        ("province_state", "string", "province_state", "string"),
        ("country_region", "string", "country_region", "string"),
        ("lat", "string", "lat", "string"),
        ("long_", "string", "long_", "string"),
        ("combined_key", "string", "combined_key", "string"),
        ("date", "string", "date", "string"),
        ("confirmed", "string", "confirmed", "string"),
        ("deaths", "string", "deaths", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Data Catalog table
DataCatalogtable_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="bronze",
    table_name="usa_county_wise",
    transformation_ctx="DataCatalogtable_node3",
)

job.commit()
