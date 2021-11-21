import boto3
import time
import json

profilejobname = 'salesdataprofilejob'
client = boto3.client('databrew')

response = client.start_job_run(Name=profilejobname)
runid = response["RunId"]
print(runid)

response = client.describe_job_run(
    Name=profilejobname,
    RunId=runid
)

status = response["State"]

while status != "SUCCEEDED":
    time.sleep(10)
    response = client.describe_job_run(Name=profilejobname, RunId=runid)
    status = response["State"]
    print("status - " + status + " job in progress...")
    

bucketname = ""
filename = ""
for o in response["Outputs"]:
    bucketname = o["Location"]["Bucket"]
    if "dq-validation" in o["Location"]["Key"]:
        filename = o["Location"]["Key"]
print(bucketname)
print(filename)

s3 = boto3.resource('s3')

content_object = s3.Object(bucketname, filename)
file_content = content_object.get()['Body'].read().decode('utf-8')
profilejson = json.loads(file_content)

for rs in profilejson["rulesetResults"]:
    print ('The %s evaluation is %s with the following details:' % (rs["name"], rs["status"]))
    for r in rs["ruleResults"]:
        print ('%s status = %s' % (r["name"], r["status"]))
