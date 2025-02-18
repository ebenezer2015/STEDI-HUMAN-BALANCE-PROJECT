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

# Script generated for node accelerometer
accelerometer_node1739364454356 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-for-udacity-project/accelerometer/"], "recurse": True}, transformation_ctx="accelerometer_node1739364454356")

# Script generated for node accelerometer_landing
EvaluateDataQuality().process_rows(frame=accelerometer_node1739364454356, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739358730044", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
accelerometer_landing_node1739364582513 = glueContext.getSink(path="s3://stedi-lake-house-for-udacity-project/accelerometer/landing/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="accelerometer_landing_node1739364582513")
accelerometer_landing_node1739364582513.setCatalogInfo(catalogDatabase="stediprojecttask_",catalogTableName="accelerometer_landing1")
accelerometer_landing_node1739364582513.setFormat("json")
accelerometer_landing_node1739364582513.writeFrame(accelerometer_node1739364454356)
job.commit()