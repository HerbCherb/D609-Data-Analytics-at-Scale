import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

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

# Script generated for node Customer Trusted
CustomerTrusted_node1758743938607 = glueContext.create_dynamic_frame.from_catalog(database="jonwgu", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1758743938607")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1758743960668 = glueContext.create_dynamic_frame.from_catalog(database="jonwgu", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1758743960668")

# Script generated for node Join
CustomerTrusted_node1758743938607DF = CustomerTrusted_node1758743938607.toDF()
AccelerometerLanding_node1758743960668DF = AccelerometerLanding_node1758743960668.toDF()
Join_node1758744050580 = DynamicFrame.fromDF(CustomerTrusted_node1758743938607DF.join(AccelerometerLanding_node1758743960668DF, (CustomerTrusted_node1758743938607DF['email'] == AccelerometerLanding_node1758743960668DF['user']), "leftsemi"), glueContext, "Join_node1758744050580")

# Script generated for node Drop Fields
DropFields_node1758744106271 = DropFields.apply(frame=Join_node1758744050580, paths=["user", "timestamp", "x", "y", "z", "serialnumber"], transformation_ctx="DropFields_node1758744106271")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=DropFields_node1758744106271, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758743898611", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1758744190646 = glueContext.getSink(path="s3://jonwgu/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1758744190646")
CustomerCurated_node1758744190646.setCatalogInfo(catalogDatabase="jonwgu",catalogTableName="customer_curated")
CustomerCurated_node1758744190646.setFormat("glueparquet", compression="snappy")
CustomerCurated_node1758744190646.writeFrame(DropFields_node1758744106271)
job.commit()
