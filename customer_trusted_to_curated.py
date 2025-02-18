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

# Script generated for node customer_trusted
customer_trusted_node1739363051967 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-for-udacity-project/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1739363051967")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1739445043804 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-lake-house-for-udacity-project/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1739445043804")

# Script generated for node CustomerFilter
SqlQuery0 = '''
select * from myDataSource1
where email in (select distinct user from myDataSource)
'''
CustomerFilter_node1739445212976 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":accelerometer_trusted_node1739445043804, "myDataSource1":customer_trusted_node1739363051967}, transformation_ctx = "CustomerFilter_node1739445212976")

# Script generated for node curated_customer
EvaluateDataQuality().process_rows(frame=CustomerFilter_node1739445212976, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1739358730044", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
curated_customer_node1739363304581 = glueContext.getSink(path="s3://stedi-lake-house-for-udacity-project/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="gzip", enableUpdateCatalog=True, transformation_ctx="curated_customer_node1739363304581")
curated_customer_node1739363304581.setCatalogInfo(catalogDatabase="stediprojecttask_",catalogTableName="curated_customer1")
curated_customer_node1739363304581.setFormat("json")
curated_customer_node1739363304581.writeFrame(CustomerFilter_node1739445212976)
job.commit()