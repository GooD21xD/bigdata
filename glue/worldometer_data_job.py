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
    table_name="worldometer_data",
    transformation_ctx="DataCatalogtable_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=DataCatalogtable_node1,
    mappings=[
        ("country_region", "string", "country_region", "string"),
        ("continent", "string", "continent", "string"),
        ("population", "string", "population", "string"),
        ("totalcases", "string", "totalcases", "string"),
        ("newcases", "string", "newcases", "string"),
        ("totaldeaths", "string", "totaldeaths", "string"),
        ("newdeaths", "string", "newdeaths", "string"),
        ("totalrecovered", "string", "totalrecovered", "string"),
        ("newrecovered", "string", "newrecovered", "string"),
        ("activecases", "string", "activecases", "string"),
        ("serious_critical", "string", "serious_critical", "string"),
        ("tot_cases_1m_pop", "string", "tot_cases_1m_pop", "string"),
        ("deaths_1m_pop", "string", "deaths_1m_pop", "string"),
        ("totaltests", "string", "totaltests", "string"),
        ("tests_1m_pop", "string", "tests_1m_pop", "string"),
        ("who_region", "string", "who_region", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Data Catalog table
DataCatalogtable_node3 = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="bronze",
    table_name="worldometer_data",
    transformation_ctx="DataCatalogtable_node3",
)

job.commit()
