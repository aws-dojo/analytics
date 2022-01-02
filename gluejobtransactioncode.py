import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

db = "dojodb"
tbl = "src_postgres_public_customers"
tx_id = glueContext.start_transaction(False)

datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database = db, table_name = tbl, 
    transformation_ctx = "datasource0")

dest_path = "s3://dojo-dataset/customers/"

sink = glueContext.getSink(
    connection_type="s3", path=dest_path,
    enableUpdateCatalog=True,
    transactionId=tx_id,
	additional_options={
            "callDeleteObjectsOnCancel":"true"
        }
)
sink.setFormat("glueparquet")
sink.setCatalogInfo(
    catalogDatabase=db, catalogTableName="customers_governed"
)

try:
    sink.writeFrame(datasource0)
    glueContext.commit_transaction(tx_id)
except Exception:
    glueContext.cancel_transaction(tx_id)
    raise
job.commit()