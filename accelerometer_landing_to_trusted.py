import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1758729347828 = glueContext.create_dynamic_frame.from_catalog(database="jonwgu", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1758729347828")

# Script generated for node Customer Trusted
CustomerTrusted_node1758729381575 = glueContext.create_dynamic_frame.from_catalog(database="jonwgu", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1758729381575")

# Script generated for node Join
Join_node1758729408799 = Join.apply(frame1=AccelerometerLanding_node1758729347828, frame2=CustomerTrusted_node1758729381575, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1758729408799")

# Script generated for node Drop Fields
DropFields_node1758729429441 = DropFields.apply(frame=Join_node1758729408799, paths=["email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate", "customername"], transformation_ctx="DropFields_node1758729429441")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1758729475431 = glueContext.getSink(path="s3://jonwgu/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1758729475431")
AccelerometerTrusted_node1758729475431.setCatalogInfo(catalogDatabase="jonwgu",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1758729475431.setFormat("json")
AccelerometerTrusted_node1758729475431.writeFrame(DropFields_node1758729429441)
job.commit()
