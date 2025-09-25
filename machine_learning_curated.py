import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1758831816890 = glueContext.create_dynamic_frame.from_catalog(database="jonwgu", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1758831816890")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1758831813770 = glueContext.create_dynamic_frame.from_catalog(database="jonwgu", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1758831813770")

# Script generated for node SQL Query
SqlQuery0 = '''
SELECT  s.serialnumber,
  s.sensorreadingtime,
  s.distancefromobject,
  a.user,
  a.timestamp,
  a.x,
  a.y,
  a.z
FROM step_trainer_trusted s
JOIN accelerometer_trusted a
  ON s.sensorreadingtime = a.timestamp;
'''
SQLQuery_node1758832048180 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_trusted":AccelerometerTrusted_node1758831813770, "step_trainer_trusted":StepTrainerTrusted_node1758831816890}, transformation_ctx = "SQLQuery_node1758832048180")

# Script generated for node Amazon S3
AmazonS3_node1758832495410 = glueContext.getSink(path="s3://jonwgu/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1758832495410")
AmazonS3_node1758832495410.setCatalogInfo(catalogDatabase="jonwgu",catalogTableName="machine_learning_curated")
AmazonS3_node1758832495410.setFormat("json")
AmazonS3_node1758832495410.writeFrame(SQLQuery_node1758832048180)
job.commit()
