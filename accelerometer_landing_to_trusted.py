import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node CustomerTrusted
CustomerTrusted_node1739369014559 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-for-udacity-project/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1739369014559")

# Script generated for node AccelerometerLanding
AccelerometerLanding_node1739368889445 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-for-udacity-project/accelerometer/landing/"], "recurse": True}, transformation_ctx="AccelerometerLanding_node1739368889445")

# Script generated for node TableJoin
TableJoin_node1739370565106 = Join.apply(frame1=CustomerTrusted_node1739369014559, frame2=AccelerometerLanding_node1739368889445, keys1=["email"], keys2=["user"], transformation_ctx="TableJoin_node1739370565106")

# Script generated for node AccelerometerTrusted
EvaluateDataQuality().process_rows(frame=TableJoin_node1739370565106, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739368873845", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AccelerometerTrusted_node1739369269822 = glueContext.getSink(path="s3://stedi-lake-house-for-udacity-project/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1739369269822")
AccelerometerTrusted_node1739369269822.setCatalogInfo(catalogDatabase="stediprojecttask_",catalogTableName="accelerometer_trusted1")
AccelerometerTrusted_node1739369269822.setFormat("json")
AccelerometerTrusted_node1739369269822.writeFrame(TableJoin_node1739370565106)
job.commit()