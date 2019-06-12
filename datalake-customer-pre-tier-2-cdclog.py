{\rtf1\ansi\ansicpg1252\cocoartf1504\cocoasubrtf830
{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww28600\viewh17520\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 import boto3\
from collections import defaultdict\
from operator import attrgetter\
import pandas as pd\
from io import BytesIO\
from io import StringIO\
\
##  AUTHORED BY dhawalkp@amazon.com\
##  This Glue Job does the pre-sequencing of the DMS CDC Log file before it it is re-partitioned based on the primary key.\
##  The Pre-sequencing is basically adding a new Column that increments automatically to tag each DMS CDC Log record with a sequence number\
##  so that the tier-2 compaction job can perform the compaction based on the latest sequence number to retain the latest data. This will also be\
##  be useful in maintaining the lifecycle of the data changes as well.\
##  The sequencing job is simply using panda library to create a data frame from S3 Object which is in Csv format and storing the sequenced csv back on S3.\
\
s3 = boto3.client('s3')\
\
## Constants and Configuration Variables\
CONST_BUCKET_NAME='octank-energy-datalake-west'\
CONST_PREFIX_NAME='Tier-1/Customer/CDC/2019/01/02'\
CONST_DESTINATION_BUCKET_NAME='octank-energy-datalake-west'\
CONST_DESTINATION_PREFIX_NAME='Pre-Tier-2/Customer/CDC/2019/01/02'\
CONST_DESTINATION_FILE_NAME='Energy_Customers_CDC_U_2019_01_02.csv'\
CONST_EXCLUDE_NAME='$folder$'\
\
\
\
## This function returns the S3 Object Content based on the matching Key using prefix, exclusion and bucket name\
\
def get_matching_s3_keys_and_objects(bucket, prefix='', suffix='',exclude=''):\
    \
    """\
    Generate the keys in an S3 bucket.\
\
    :param bucket: Name of the S3 bucket.\
    :param prefix: Only fetch keys that start with this prefix (optional).\
    :param suffix: Only fetch keys that end with this suffix (optional).\
    """\
    kwargs = \{'Bucket': bucket\}\
    \
    # If the prefix is a single string (not a tuple of strings), we can\
    # do the filtering directly in the S3 API.\
    if isinstance(prefix, str):\
        kwargs['Prefix'] = prefix\
        \
    while True:\
\
        # The S3 API response is a large blob of metadata.\
        # 'Contents' contains information about the listed objects.\
        resp = s3.list_objects_v2(**kwargs)\
        for obj in resp['Contents']:\
            print obj\
            key = obj['Key']\
            \
            if key.startswith(prefix) and key.endswith(suffix) and not exclude in key:\
                \
                obj = s3.get_object(Bucket=CONST_BUCKET_NAME, Key=key)\
                yield obj['Body']\
\
        # The S3 API is paginated, returning up to 1000 keys at a time.\
        # Pass the continuation token into the next response, until we\
        # reach the final page (when this field is missing).\
        try:\
            kwargs['ContinuationToken'] = resp['NextContinuationToken']\
        except KeyError:\
            break\
\
## The below function writes the Panda Dataframe which is in CSV into the S3 Bucket\
\
def _write_dataframe_to_csv_on_s3(dataframe, filename):\
    """ Write a dataframe to a CSV on S3 """\
    print("Writing \{\} records to \{\}".format(len(dataframe), filename))\
    # Create buffer\
    csv_buffer = BytesIO()\
    # Write dataframe to buffer\
    dataframe.to_csv(csv_buffer, sep=",", index=False)\
    # Create S3 object\
    s3_resource = boto3.resource("s3")\
    # Write buffer to S3 object\
    s3_resource.Object(CONST_DESTINATION_BUCKET_NAME, filename).put(Body=csv_buffer.getvalue())\
\
\
dmsObject = pd.DataFrame()\
## Get all the Object Contents based on the prefix and transform each Object (csv) by adding a new column named "New_ID" as a auto-sequence.\
for body in get_matching_s3_keys_and_objects(bucket=CONST_BUCKET_NAME, prefix=CONST_PREFIX_NAME, suffix='', exclude=CONST_EXCLUDE_NAME):\
    csv_string = body.read().decode('utf-8')\
    dmsObject = pd.read_csv(StringIO(csv_string))\
    dmsObject.insert(0, 'New_ID', range(0, 0 + len(dmsObject)))\
\
## Writes the transformed CSV object based on the bucket, with pre-tier-2 prefix\
_write_dataframe_to_csv_on_s3(dmsObject, CONST_DESTINATION_PREFIX_NAME+'/'+CONST_DESTINATION_FILE_NAME)\
\
}