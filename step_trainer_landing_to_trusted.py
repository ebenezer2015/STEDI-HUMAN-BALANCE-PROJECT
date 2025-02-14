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
CustomerTrusted_node1739376053845 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-for-udacity-project/customer/trusted/"], "recurse": True}, transformation_ctx="CustomerTrusted_node1739376053845")

# Script generated for node StepTrainerLanding
StepTrainerLanding_node1739375760002 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-for-udacity-project/step_trainer/landing/"], "recurse": True}, transformation_ctx="StepTrainerLanding_node1739375760002")

# Script generated for node Join
Join_node1739376115712 = Join.apply(frame1=CustomerTrusted_node1739376053845, frame2=StepTrainerLanding_node1739375760002, keys1=["serialNumber"], keys2=["serialNumber"], transformation_ctx="Join_node1739376115712")

# Script generated for node StepTrainerTrusted
EvaluateDataQuality().process_rows(frame=Join_node1739376115712, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739368873845", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1739376306719 = glueContext.write_dynamic_frame.from_options(frame=Join_node1739376115712, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lake-house-for-udacity-project/step_trainer/trusted/", "compression": "gzip", "partitionKeys": []}, transformation_ctx="StepTrainerTrusted_node1739376306719")

job.commit()