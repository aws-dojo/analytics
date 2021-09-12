import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())

df = glueContext.create_dynamic_frame.from_catalog(
             database="dojodb",
             table_name="customers")
df.printSchema()

df = SelectFields.apply(df,["contactfirstname","contactlastname"])
df.printSchema()

glueContext.write_dynamic_frame.from_options(df, connection_type = "s3", connection_options = {"path": "s3://dojo-dataset/output/"}, format = "csv")

df = glueContext.create_dynamic_frame.from_catalog(
             database="dojodb",
             table_name="dev_public_users", redshift_tmp_dir="s3://dojo-dataset/temp/")
df.printSchema()

df = SelectFields.apply(df,["username","firstname","lastname"])
df.printSchema()

glueContext.write_dynamic_frame.from_jdbc_conf(df, catalog_connection = "redshiftconnection",
                                                   connection_options = {"dbtable": "usermini", "database": "dev"},
                                                   redshift_tmp_dir = "s3://dojo-dataset/temp/")

df = glueContext.create_dynamic_frame.from_catalog(
             database="dojodb",
             table_name="postgres_public_employees", redshift_tmp_dir="s3://dojo-dataset/temp/")
df.printSchema()

df = SelectFields.apply(df,["firstname","lastname"])
df.printSchema()

glueContext.write_dynamic_frame.from_jdbc_conf(df, catalog_connection = "rdsconnection",
                                                   connection_options = {"dbtable": "employeesmini", "database": "postgres"},
                                                   redshift_tmp_dir = "s3://dojo-dataset/temp/")

glueContext.write_dynamic_frame.from_options(df, connection_type = "s3", connection_options = {"path": "s3://dojo-dataset/rdsoutput/"}, format = "csv")


