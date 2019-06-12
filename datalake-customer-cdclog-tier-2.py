{\rtf1\ansi\ansicpg1252\cocoartf1504\cocoasubrtf830
{\fonttbl\f0\fnil\fcharset0 HelveticaNeue;}
{\colortbl;\red255\green255\blue255;\red0\green0\blue0;\red233\green255\blue255;}
{\*\expandedcolortbl;;\cssrgb\c0\c0\c0;\cssrgb\c92941\c100000\c100000;}
\margl1440\margr1440\vieww28600\viewh17520\viewkind0
\deftab720
\pard\pardeftab720\sl320\partightenfactor0

\f0\fs28 \cf2 \cb3 \expnd0\expndtw0\kerning0
\outl0\strokewidth0 \strokec2 import sys\
import boto3\
from awsglue.transforms import *\
from awsglue.utils import getResolvedOptions\
from pyspark.context import SparkContext\
from awsglue.context import GlueContext\
from awsglue.job import Job\
from awsglue.dynamicframe import DynamicFrame\
from pyspark.sql.types import *\
\
\
\
\
## @params: [JOB_NAME]\
args = getResolvedOptions(sys.argv, ['JOB_NAME'])\
\
CONST_SRC_CATALOG_DB='octank-energy-datalake-tier-2-west'\
CONST_SRC_CATALOG_PRE_TIER2_TABLE='cdc'\
CONST_CATALOG_TIER2_TABLE='tier_2customer'\
\
CONST_TARGET_CATALOG_DB='octank-energy-datalake-tier-2-west'\
\
\
CONST_BUCKET_NAME='octank-energy-datalake-west'\
CONST_PREFIX_NAME='Tier-2/Customer/'\
CONST_PARTITION_KEY='customerid'\
\
sc = SparkContext()\
glueContext = GlueContext(sc)\
spark = glueContext.spark_session\
job = Job(glueContext)\
job.init(args['JOB_NAME'], args)\
botoClient = boto3.client('glue',region_name='us-west-2')\
## @type: DataSource\
## @args: [database = "employee-tier1", table_name = "cdc", transformation_ctx = "datasource0"]\
## @return: datasource0\
## @inputs: []\
## Dynamic Frame for the CDC Tier1 Records\
pushDownPredicate = "(year=='2019' and month=='01' and day=='01')"\
\
customerCDCDF = glueContext.create_dynamic_frame.from_catalog(database = CONST_SRC_CATALOG_DB, table_name = CONST_SRC_CATALOG_PRE_TIER2_TABLE,push_down_predicate=pushDownPredicate, transformation_ctx = "customerCDCDF")\
\
\
mappedCustomerDF = ApplyMapping.apply(frame = customerCDCDF, mappings = [("new_id", "bigint", "new_id", "bigint"),("op", "string", "op", "string"), ("customerid", "string", "customerid", "string"), ("email", "string", "email", "string"), ("lclid", "string", "lclid", "string")],transformation_ctx = "mappedCustomerDF")\
sparkCustomerCDCDF = mappedCustomerDF.toDF();\
\
\
for customerInstance in sparkCustomerCDCDF.rdd.collect():\
    print "Processing started for RDDs.."\
\
\
    ## The below logic is only processing the 'U' Log records of CDC. Similar logic can be written by segregrating the\
    ##  below logic into a python function and making it a seperate re-usable. The "D" Record needs to Delete partition by\
    ##  deleting the catalog entry and the underlying S3 objects.\
\
    if customerInstance["op"] == "U":\
        print "Processing Update record..",customerInstance[CONST_PARTITION_KEY]\
        pushDownCustomerIdPredicate = "(customerid=='"+str(customerInstance[CONST_PARTITION_KEY])+"')"\
        customerFinalDF = glueContext.create_dynamic_frame.from_catalog(database = CONST_TARGET_CATALOG_DB, table_name = CONST_CATALOG_TIER2_TABLE, push_down_predicate=pushDownCustomerIdPredicate, transformation_ctx = "customerFinalDF")\
        if customerFinalDF.count() > 0:\
\
\
            tempDF = spark.createDataFrame([customerInstance])\
            columns_to_drop = ['op','year', 'month','day']\
            tempDF=tempDF.drop(*columns_to_drop)\
            tempDynF = DynamicFrame.fromDF(tempDF,glueContext,"tempDynF")\
            ##objWritten = tempDF.write.format("csv").partitionBy(CONST_PARTITION_KEY).option("compression", "gzip").mode("Append").save("s3a://"+CONST_BUCKET_NAME+"/"+CONST_PREFIX_NAME);\
            #print "Object Written " \
            #print objWritten\
            ##tempDF.write.partitionBy(CONST_PARTITION_KEY).option("name", "dhawal").option("lastmodifiedtimestamp", "XXXX").csv("s3://"+CONST_BUCKET_NAME+"/"+CONST_PREFIX_NAME);\
            ##employeeUpdateSinkDF = glueContext.write_dynamic_frame.from_options(frame = tempDynF, connection_type = "s3", connection_options = \{"path": "s3://"+CONST_BUCKET_NAME+"/"+CONST_PREFIX_NAME,"partitionKeys": [CONST_PARTITION_KEY]\}, format = "csv", transformation_ctx = "employeeUpdateSinkDF")\
            customerUpdateSinkDF = glueContext.write_dynamic_frame.from_options(frame = tempDynF, connection_type = "s3", connection_options = \{"path": "s3://"+CONST_BUCKET_NAME+"/"+CONST_PREFIX_NAME,"partitionKeys": [CONST_PARTITION_KEY]\}, format = "csv", transformation_ctx = "customerUpdateSinkDF")\
            print "Updated the record ", customerInstance[CONST_PARTITION_KEY]\
\
\
job.commit()\
}