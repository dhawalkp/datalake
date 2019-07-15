import boto3
from collections import defaultdict
from operator import attrgetter


##  AUTHORED BY dhawalkp@amazon.com
## This AWS Glue Python Script does the "Compaction" of the Mutable Data Objects based on S3 Object's LastModificationTime. 
## The Compaction script deletes the old objects. The script can be scheduled periodically via AWS CloudWatch as well. 
## This script needs performance tuned to reduce Memory and Time Complexity. The Grouping and Parsing the list multiple times increases the Big O. 
## Used Multi-Deletes in Batch to reduce calls to S3 as far as possible. WOuld be great if we can reduce the dict/list processing and do in-line in 
## get_matching_s3_keys function.
## DO NOT use this for production use directly without reviewing some of the perf considerations listed above.
## This also needs to be enhanced to process 'D' Records of AWS DMS by adding logic to also make corresponding calls to AWS Glue to delete the partition in Data Catalog
## This sample processes the Employee table CDC/Full record emitted by AWS DMS from AWS RDS - ORacle. The Employee Record format is as below -
## Op	EMPLOYEE_ID	FIRST_NAME	LAST_NAME	EMAIL	PHONE_NUMBER	HIRE_DATE	JOB_ID	SALARY	COMMISSION_PCT	MANAGER_ID	DEPARTMENT_ID
## I	1651	William1651	Gietz	WGIETZ1651	515.123.8181	6/7/94 0:00	AC_ACCOUNT	8300		205	110
## The Format of AWS DMS log record can be found at https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.S3.html 

s3 = boto3.client('s3')

## Constants and Configuration Variables
## SOURCE BUCKET NAME
CONST_BUCKET_NAME='octank-energy-datalake-west'
## SOURCE OBJECT PREFIX
CONST_PREFIX_NAME='Tier-2/Customer/customerid'
CONST_SUFFIX_NAME=''
## SUFFIXES NEEDS TO BE EXCLUDED
CONST_EXCLUDE_NAME='$folder$'
CONST_LASTMODIFIEDTIME_STR='lastModifiedTime'
CONST_LASTMODIFIEDATTR_STR='LastModified'

## This is a Class to model the DMS record
## file -> Key of S3 Object
## lastModifiedTime -> Last Modified Time in millis since epoch
## type -> type of the DMS Record i.e. U/I/D
## This script currently handles U CDC records only. The script needs to be enhanced to handle other records.

class Tuple:
    def __init__(self, file, lastModifiedTime, type):
        self.file = file
        self.lastModifiedTime = lastModifiedTime
        self.type = type
    def getTuple(self):
        return self
    def __str__(self):
        return self.file+" "+str(self.lastModifiedTime)+ " "+ self.type


## This function returns the Python Tuple in below format based on BucketName, Prefix, Exclude, Prefix and Suffix Exp
## Python Tuple = (ParitionKey, Custom Object "Tuple" - DMS Log Record)

def get_matching_s3_keys(bucket, prefix='', suffix='',exclude=''):
    
    """
    Generate the keys in an S3 bucket.

    :param bucket: Name of the S3 bucket.
    :param prefix: Only fetch keys that start with this prefix (optional).
    :param suffix: Only fetch keys that end with this suffix (optional).
    """
    kwargs = {'Bucket': bucket}

    # If the prefix is a single string (not a tuple of strings), we can
    # do the filtering directly in the S3 API.
    if isinstance(prefix, str):
        kwargs['Prefix'] = prefix
        
    while True:

        # The S3 API response is a large blob of metadata.
        # 'Contents' contains information about the listed objects.
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp['Contents']:
            key = obj['Key']
            
            if key.startswith(prefix) and key.endswith(suffix) and not exclude in key:
                # Returning a list with PartitionKey and Last Modified time in secs since epoch (%s)
                
                Obj = Tuple(key,obj[CONST_LASTMODIFIEDATTR_STR].strftime('%s'),'U')
                tupleItem = ()
              
                tupleItem = (key.split('/')[2],Obj)
                
                
                yield tupleItem

        # The S3 API is paginated, returning up to 1000 keys at a time.
        # Pass the continuation token into the next response, until we
        # reach the final page (when this field is missing).
        try:
            kwargs['ContinuationToken'] = resp['NextContinuationToken']
        except KeyError:
            break


## Idea is to Group by PartitionKey and get below Dictionary so that we can find the latest S3 Object based on lastModifiedTime
## {PartitionKey: [Tuple Objects]}

partitionList = []
groups = {}
keys_to_delete = []
s3objects_to_delete = [];
## create a list of Tuples
for items in get_matching_s3_keys(bucket=CONST_BUCKET_NAME, prefix=CONST_PREFIX_NAME, suffix='', exclude=CONST_EXCLUDE_NAME):
    partitionList.append(items)


##Group by on the key to get lastest modified timestamp and delete the rest
groups = defaultdict(list)
for k,v in partitionList:
    groups[k].append(v)

##Delete the old S3 objects and keep the latest based on the timestamp
for key,TupleList in groups.iteritems():
    

    
    keyToRetain=max(TupleList,key=attrgetter(CONST_LASTMODIFIEDTIME_STR)).file
    
    
    
    for Object in TupleList:
        if Object.file != keyToRetain:
            keys_to_delete.append(Object.file)
            
            


## Delete the old S3 objects in a batch of 1000 since batch delete has a limit of same.
## Showing the logic to delete the partition after processing 'D' DMS record as well
index=0

for itemsToDelete in keys_to_delete:
    #print itemsToDelete
    s3Object = {}
    s3Object['Key'] = itemsToDelete
    s3objects_to_delete.append(s3Object)
    if index % 999 == 0:
        
        S3ObjectDict = {}
        S3ObjectDict['Objects']=s3objects_to_delete
        s3.delete_objects(Bucket=CONST_BUCKET_NAME, Delete=S3ObjectDict)

        s3objects_to_delete=[]
        
        
    index = index + 1 

## Deleting the residual Records

S3ObjectDict = {}
S3ObjectDict['Objects']=s3objects_to_delete
print("Final Objects to  be deleted are: ")
print(len(S3ObjectDict['Objects']))
s3.delete_objects(Bucket=CONST_BUCKET_NAME, Delete=S3ObjectDict)

