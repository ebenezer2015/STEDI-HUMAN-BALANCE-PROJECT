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

# Script generated for node step_trainer
step_trainer_node1739363051967 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-for-udacity-project/step_trainer/"], "recurse": True}, transformation_ctx="step_trainer_node1739363051967")

# Script generated for node step_trainer landing
EvaluateDataQuality().process_rows(frame=step_trainer_node1739363051967, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739358730044", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainerlanding_node1739363304581 = glueContext.getSink(path="s3://stedi-lake-house-for-udacity-project/step_trainer/landing/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="step_trainerlanding_node1739363304581")
step_trainerlanding_node1739363304581.setCatalogInfo(catalogDatabase="stediprojecttask_",catalogTableName="step_trainer_landing1")
step_trainerlanding_node1739363304581.setFormat("json")
step_trainerlanding_node1739363304581.writeFrame(step_trainer_node1739363051967)
job.commit()