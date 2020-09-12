# Glue Script to read from S3, filter data and write to Dynamo DB.
# First read S3 data using Spark Context, Glue Context can also be used. Using Spark Context just to illustrate that
# dataframe can be conveted to dynamic filter.
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from pyspark import SparkContext

args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_file_path', 'dynamodb_table'])
sc = SparkContext()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args['JOB_NAME'], args)

s3_file_path = args['s3_file_path']
dynamodb_table = args['dynamodb_table']

df_src = spark.read.format("csv").option("header", "true").load(s3_file_path)
df_fil_dept = df_src.filter(df_src.dept_no == 101)

dyf_result = DynamicFrame.fromDF(df_fil_dept, glue_ctx, "dyf_result")

glue_ctx. \
    write_dynamic_frame_from_options(
        frame=dyf_result, connection_type="dynamodb",
        connection_options={"dynamodb.output.tableName": dynamodb_table, "dynamodb.throughput.write.percent": "1.0"})

job.commit()
