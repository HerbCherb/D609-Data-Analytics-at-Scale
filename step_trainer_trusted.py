import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Step trainer Landing
SteptrainerLanding_node1758808112025 = glueContext.create_dynamic_frame.from_catalog(database="jonwgu", table_name="step_trainer_landing", transformation_ctx="SteptrainerLanding_node1758808112025")

# Script generated for node Customer Curated
CustomerCurated_node1758834631737 = glueContext.create_dynamic_frame.from_catalog(database="jonwgu", table_name="customer_curated", transformation_ctx="CustomerCurated_node1758834631737")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT DISTINCT s.*
FROM step_trainer_landing AS s
JOIN customer_curated AS c
  ON s.serialnumber = c.serialNumber;
'''
SQLQuery_node1758831551749 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer_landing":SteptrainerLanding_node1758808112025, "customer_curated":CustomerCurated_node1758834631737}, transformation_ctx = "SQLQuery_node1758831551749")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1758831551749, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758819182928", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1758823407151 = glueContext.getSink(path="s3://jonwgu/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1758823407151")
AmazonS3_node1758823407151.setCatalogInfo(catalogDatabase="jonwgu",catalogTableName="step_trainer_trusted")
AmazonS3_node1758823407151.setFormat("json")
AmazonS3_node1758823407151.writeFrame(SQLQuery_node1758831551749)
job.commit()
