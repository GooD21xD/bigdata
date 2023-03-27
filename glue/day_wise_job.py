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
    table_name="day_wise",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("date", "string", "date", "string"),
        ("confirmed", "string", "confirmed", "string"),
        ("deaths", "string", "deaths", "string"),
        ("recovered", "string", "recovered", "string"),
        ("active", "string", "active", "string"),
        ("new_cases", "string", "new_cases", "string"),
        ("new_deaths", "string", "new_deaths", "string"),
        ("new_recovered", "string", "new_recovered", "string"),
        ("deaths_100_cases", "string", "deaths_100_cases", "string"),
        ("recovered_100_cases", "string", "recovered_100_cases", "string"),
        ("deaths_100_recovered", "string", "deaths_100_recovered", "string"),
        ("no_of_countries", "string", "no_of_countries", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Data Catalog table
DataCatalogtable_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="bronze",
    table_name="day_wise",
    transformation_ctx="DataCatalogtable_node3",
)

job.commit()