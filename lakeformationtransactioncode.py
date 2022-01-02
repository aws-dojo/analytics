#!/usr/bin/env python
# coding: utf-8

# In[1]:


import boto3
client = boto3.client('lakeformation')

objectname1 = 'run-AmazonS3_node1640170420462-2-part-block-0-r-00006-snappy.parquet'
objectname2 = 'run-AmazonS3_node1640181213980-1-part-block-0-r-00005-snappy.parquet'
bucketname = 'dojo-dataset'
sourcefolder1 = 'source/employees/'
sourcefolder2 = 'source/customers/'
destinationfolder1 = 'employees/'
destinationfolder2 = 'customers/'
accountnumber = '999999999999'
database = "dojodb"
table1 = 'employees_governed'
table2 = 'customers_governed'
etag1 = 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
etag2 = 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'
size1 = 614
size2 = 519


# In[2]:


response = client.start_transaction(TransactionType='READ_AND_WRITE')
response


# In[3]:


trx_id = response['TransactionId']
trx_id


# In[4]:


s3 = boto3.resource('s3')

copy_source = {
    'Bucket': bucketname,
    'Key': sourcefolder1 + objectname1}

s3.meta.client.copy(copy_source,bucketname, destinationfolder1 + objectname1)

copy_source = {
    'Bucket': bucketname,
    'Key': sourcefolder2 + objectname2}

s3.meta.client.copy(copy_source,bucketname, destinationfolder2 + objectname2)


# In[5]:


response = client.delete_objects_on_cancel(
    CatalogId=accountnumber,
    DatabaseName=database,
    TableName=table1,
    TransactionId=trx_id,
    Objects=[
        {
            'Uri': 's3://' + bucketname + '/' + destinationfolder1 + objectname1,
            'ETag': etag1
        }
    ]
)

response = client.delete_objects_on_cancel(
    CatalogId=accountnumber,
    DatabaseName=database,
    TableName=table2,
    TransactionId=trx_id,
    Objects=[
        {
            'Uri': 's3://' + bucketname + '/' + destinationfolder2 + objectname2,
            'ETag': etag2
        }
    ]
)


# In[6]:


response = client.update_table_objects(
    CatalogId=accountnumber,
    DatabaseName=database,
    TableName=table1,
    TransactionId=trx_id,
    WriteOperations=[
        {
            'AddObject': {
                'Uri': 's3://' + bucketname + '/' + destinationfolder1 + objectname1,
                'ETag': etag1,
                'Size': size1
            }
        }
    ]
)

response = client.update_table_objects(
    CatalogId=accountnumber,
    DatabaseName=database,
    TableName=table2,
    TransactionId=trx_id,
    WriteOperations=[
        {
            'AddObject': {
                'Uri': 's3://' + bucketname + '/' + destinationfolder2 + objectname2,
                'ETag': etag2,
                'Size': size2
            }
        }
    ]
)


# In[ ]:


response = client.commit_transaction(TransactionId=trx_id)
response


# In[7]:


response = client.cancel_transaction(TransactionId=trx_id)
response


# In[8]:


response = client.describe_transaction(TransactionId=trx_id)
response


# In[ ]:






