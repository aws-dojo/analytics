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
