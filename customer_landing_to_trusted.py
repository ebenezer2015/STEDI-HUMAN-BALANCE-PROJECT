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

# Script generated for node customerlanding
customerlanding_node1739366287601 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-for-udacity-project/customer/landing/"], "recurse": True}, transformation_ctx="customerlanding_node1739366287601")

# Script generated for node ReasearchFilter
SqlQuery2273 = '''
select * from myDataSource
where sharewithresearchasofdate is not null

'''
ReasearchFilter_node1739366401030 = sparkSqlQuery(glueContext, query = SqlQuery2273, mapping = {"myDataSource":customerlanding_node1739366287601}, transformation_ctx = "ReasearchFilter_node1739366401030")

# Script generated for node CustomerLanding To TrustedZone
EvaluateDataQuality().process_rows(frame=ReasearchFilter_node1739366401030, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739366247721", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerLandingToTrustedZone_node1739366892622 = glueContext.write_dynamic_frame.from_options(frame=ReasearchFilter_node1739366401030, connection_type="s3", format="json", connection_options={"path": "s3://stedi-lake-house-for-udacity-project/customer/trusted/", "compression": "gzip", "partitionKeys": []}, transformation_ctx="CustomerLandingToTrustedZone_node1739366892622")

job.commit()