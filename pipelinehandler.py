Job1
====
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())

customerDF = glueContext.create_dynamic_frame.from_catalog(
             database="dojodb",
             table_name="src_postgres_public_customers", redshift_tmp_dir="s3://dojo-dataset/scripts/")

glueContext.write_dynamic_frame.from_options(customerDF, connection_type = "s3", connection_options = {"path": "s3://dojo-dataset/raw/customers"}, format = "csv")

Job2
====

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())

customersDF = glueContext.create_dynamic_frame.from_catalog(
             database="dojodb",
             table_name="raw_customers")
glueContext.write_dynamic_frame.from_options(customersDF, connection_type = "s3", connection_options = {"path": "s3://dojo-dataset/cleansed/customers"}, format = "parquet")

Lambda Code
===========

import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    source = ""
    if event["detail-type"] == "Glue Crawler State Change":
        source = event["detail"]["crawlerName"]
        print(source)
    
    if event["detail-type"] == "Glue Job State Change":
        source = event["detail"]["jobName"]
        print(source)
    
    ddclient = boto3.client('dynamodb')
    ddresp = ddclient.execute_statement(Statement= "select target, targettype from pipelineconfig where source = '" + source + "'")
    target = ddresp["Items"][0]["target"]["S"]
    targettype = ddresp["Items"][0]["targettype"]["S"]
    print(target)
    print(targettype)
    
    glueclient = boto3.client('glue')
    if targettype == "crawler":
        glueclient.start_crawler(Name=target)
    if targettype == "job":
        glueclient.start_job_run(JobName=target)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Handler Called')
    }
