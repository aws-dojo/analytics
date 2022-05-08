###### Glue Job - jdbcdataingestion ###########

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME","cdb","ctbl","dest"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

db = args["cdb"]
tbl = args["ctbl"]
dest = args["dest"]

# Script generated for node Amazon Redshift
AmazonRedshift_node1651236431390 = glueContext.create_dynamic_frame.from_catalog(
    database=db,
    redshift_tmp_dir=args["TempDir"],
    table_name=tbl,
    transformation_ctx="AmazonRedshift_node1651236431390",
)

# Script generated for node Amazon S3
AmazonS3_node1651236450523 = glueContext.write_dynamic_frame.from_options(
    frame=AmazonRedshift_node1651236431390,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": dest, "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1651236450523",
)

job.commit()

###### Pipeline - Step Functions Workflow ###########

{
  "Comment": "A description of my state machine",
  "StartAt": "masterparallel",
  "States": {
    "masterparallel": {
      "Type": "Parallel",
      "Branches": [
        {
          "StartAt": "Parallel1",
          "States": {
            "Parallel1": {
              "Type": "Parallel",
              "Branches": [
                {
                  "StartAt": "IngestOrdersData",
                  "States": {
                    "IngestOrdersData": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::glue:startJobRun.sync",
                      "Parameters": {
                        "JobName": "jdbcdataingestion",
                        "Arguments": {
                          "--cdb": "dojodb",
                          "--ctbl": "dev_public_orders",
                          "--dest": "s3://my-demo-datalake/orders/"
                        }
                      },
                      "End": true
                    }
                  }
                },
                {
                  "StartAt": "IngestCustomersData",
                  "States": {
                    "IngestCustomersData": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::glue:startJobRun.sync",
                      "Parameters": {
                        "JobName": "jdbcdataingestion",
                        "Arguments": {
                          "--cdb": "dojodb",
                          "--ctbl": "dev_public_customers",
                          "--dest": "s3://my-demo-datalake/customers/"
                        }
                      },
                      "End": true
                    }
                  }
                }
              ],
              "End": true
            }
          }
        },
        {
          "StartAt": "Parallel2",
          "States": {
            "Parallel2": {
              "Type": "Parallel",
              "End": true,
              "Branches": [
                {
                  "StartAt": "IngestCustomerProfileData",
                  "States": {
                    "IngestCustomerProfileData": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::glue:startJobRun.sync",
                      "Parameters": {
                        "JobName": "jdbcdataingestion",
                        "Arguments": {
                          "--cdb": "dojodb",
                          "--ctbl": "dev_public_customerprofile",
                          "--dest": "s3://my-demo-datalake/customerprofile/"
                        }
                      },
                      "End": true
                    }
                  }
                },
                {
                  "StartAt": "IngestSensorData",
                  "States": {
                    "IngestSensorData": {
                      "Type": "Task",
                      "Resource": "arn:aws:states:::glue:startJobRun.sync",
                      "Parameters": {
                        "JobName": "jdbcdataingestion",
                        "Arguments": {
                          "--cdb": "dojodb",
                          "--ctbl": "dev_public_sensordata",
                          "--dest": "s3://my-demo-datalake/sensordata/"
                        }
                      },
                      "End": true
                    }
                  }
                }
              ]
            }
          }
        }
      ],
      "End": true
    }
  }
}
