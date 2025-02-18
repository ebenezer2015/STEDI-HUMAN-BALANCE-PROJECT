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

# Script generated for node customer-to-landing
customertolanding_node1739355492003 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-for-udacity-project/customer/"], "recurse": True}, transformation_ctx="customertolanding_node1739355492003")

# Script generated for node Landing Zone
EvaluateDataQuality().process_rows(frame=customertolanding_node1739355492003, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739356853920", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
LandingZone_node1739356920015 = glueContext.getSink(path="s3://stedi-lake-house-for-udacity-project/customer/landing/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="LandingZone_node1739356920015")
LandingZone_node1739356920015.setCatalogInfo(catalogDatabase="stediprojecttask_",catalogTableName="customer_landing1")
LandingZone_node1739356920015.setFormat("json")
LandingZone_node1739356920015.writeFrame(customertolanding_node1739355492003)
job.commit()