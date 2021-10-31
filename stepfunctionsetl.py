#job1 *****************

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

#job2 ***********************

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

#start-crawler-lambda *****************

import json
import boto3

def lambda_handler(event, context):
    target = event["crawlername"]
    glueclient = boto3.client('glue')
    glueclient.start_crawler(Name=target)

#get-crawler-state-lambda ***************

import json
import boto3

def lambda_handler(event, context):
    target = event["crawlername"]
    glueclient = boto3.client('glue')
    response = glueclient.get_crawler(Name=target)
    return {
        'state': response['Crawler']['State']
    }

#step function json *****************

{
  "Comment": "A description of my state machine",
  "StartAt": "job1_start",
  "States": {
    "job1_start": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "job1"
      },
      "Next": "crawler1_start"
    },
    "crawler1_start": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:eu-west-1:<account_number>:function:startcrawlerfunction:$LATEST",
        "Payload": {
          "crawlername": "crawler1"
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 6,
          "BackoffRate": 2
        }
      ],
      "Next": "get_crawler1_state"
    },
    "get_crawler1_state": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:eu-west-1:<account_number>:function:getcrawlerfunction:$LATEST",
        "Payload": {
          "crawlername": "crawler1"
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 6,
          "BackoffRate": 2
        }
      ],
      "Next": "Choice"
    },
    "Choice": {
      "Type": "Choice",
      "Choices": [
        {
          "Not": {
            "Variable": "$.state",
            "StringEquals": "READY"
          },
          "Next": "Wait"
        }
      ],
      "Default": "job2_start"
    },
    "Wait": {
      "Type": "Wait",
      "Seconds": 3,
      "Next": "get_crawler1_state"
    },
    "job2_start": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "job2"
      },
      "Next": "crawler2_start"
    },
    "crawler2_start": {
      "Type": "Task",
      "Resource": "arn:aws:states:::lambda:invoke",
      "OutputPath": "$.Payload",
      "Parameters": {
        "FunctionName": "arn:aws:lambda:eu-west-1:<account_number>:function:startcrawlerfunction:$LATEST",
        "Payload": {
          "crawlername": "crawler2"
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Lambda.ServiceException",
            "Lambda.AWSLambdaException",
            "Lambda.SdkClientException"
          ],
          "IntervalSeconds": 2,
          "MaxAttempts": 6,
          "BackoffRate": 2
        }
      ],
      "End": true
    }
  }
}


