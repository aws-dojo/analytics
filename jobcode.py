import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

sensordf = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={"paths": ["s3://dojo-mydata/sensordata/"], "recurse": True, 'groupFiles': 'inPartition', 'groupSize': '1048576'}
)

sensordf = SelectFields.apply(
    frame=sensordf,
    paths=["recorddate", "failure"]
)

glueContext.write_dynamic_frame.from_options(
    frame=sensordf,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://dojo-mydata/output/", "partitionKeys": []},
    format_options={"compression": "snappy"}
)

job.commit()
