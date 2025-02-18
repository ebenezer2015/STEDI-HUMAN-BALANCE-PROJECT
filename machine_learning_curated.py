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

# Script generated for node AccelerometerTrusted
AccelerometerTrusted_node1739377892055 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-for-udacity-project/accelerometer/trusted/"], "recurse": True}, transformation_ctx="AccelerometerTrusted_node1739377892055")

# Script generated for node StepTrainerTrusted
StepTrainerTrusted_node1739377812620 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-for-udacity-project/step_trainer/trusted/"], "recurse": True}, transformation_ctx="StepTrainerTrusted_node1739377812620")

# Script generated for node Join
Join_node1739377966082 = Join.apply(frame1=AccelerometerTrusted_node1739377892055, frame2=StepTrainerTrusted_node1739377812620, keys1=["timestamp"], keys2=["sensorReadingTime"], transformation_ctx="Join_node1739377966082")

# Script generated for node machine_learning_curated
EvaluateDataQuality().process_rows(frame=Join_node1739377966082, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739368873845", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machine_learning_curated_node1739378055926 = glueContext.getSink(path="s3://stedi-lake-house-for-udacity-project/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1739378055926")
machine_learning_curated_node1739378055926.setCatalogInfo(catalogDatabase="stediprojecttask_",catalogTableName="machine_learning_curated1")
machine_learning_curated_node1739378055926.setFormat("json")
machine_learning_curated_node1739378055926.writeFrame(Join_node1739377966082)
job.commit()