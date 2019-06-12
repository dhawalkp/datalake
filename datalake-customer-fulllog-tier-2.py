{\rtf1\ansi\ansicpg1252\cocoartf1504\cocoasubrtf830
{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww28600\viewh17520\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 import sys\
import time\
import datetime\
from awsglue.transforms import *\
from awsglue.utils import getResolvedOptions\
from pyspark.context import SparkContext\
from awsglue.context import GlueContext\
from awsglue.job import Job\
\
## @params: [JOB_NAME]\
args = getResolvedOptions(sys.argv, ['JOB_NAME'])\
print "Starting the processing - ", datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')\
sc = SparkContext()\
print "SparkContext Created - ",datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')\
\
glueContext = GlueContext(sc)\
\
print "Glue Context Created - ",datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')\
\
spark = glueContext.spark_session\
\
print "Spark Session Created - ",datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')\
\
job = Job(glueContext)\
\
\
job.init(args['JOB_NAME'], args)\
\
print "Starting the Job...",datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')\
\
## @type: DataSource\
## @args: [database = "customer", table_name = "tier1_bucket", transformation_ctx = "datasource0"]\
## @return: datasource0\
## @inputs: []\
customerFullDF = glueContext.create_dynamic_frame.from_catalog(database = "octank-energy-datalake-tier-1-west", table_name = "full", transformation_ctx = "customerFullDF")\
\
print "Creating DFs - ",datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')\
\
## @type: ApplyMapping\
## @return: applymapping1\
## @inputs: [frame = datasource0]\
applymapping1 = ApplyMapping.apply(frame = customerFullDF, mappings = [ ("customerid", "string", "customerid", "string"), ("email", "string", "email", "string"), ("lclid", "string", "lclid", "string")], transformation_ctx = "applymapping1")\
\
print "Appying  the transformation - ",datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')\
\
## @type: ResolveChoice\
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]\
## @return: resolvechoice2\
## @inputs: [frame = applymapping1]\
resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")\
## @type: DropNullFields\
## @args: [transformation_ctx = "dropnullfields3"]\
## @return: dropnullfields3\
## @inputs: [frame = resolvechoice2]\
partitionedFinalDF = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "partitionedFinalDF")\
## @type: DataSink\
## @args: [connection_type = "s3", connection_options = \{"path": "s3://tier1-database-table1/Tier2_Bucket"\}, format = "parquet", transformation_ctx = "datasink4"]\
## @return: datasink4\
## @inputs: [frame = dropnullfields3]\
print "Final transformation done - ",datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')\
\
\
customerSink = glueContext.write_dynamic_frame.from_options(frame = partitionedFinalDF, connection_type = "s3", connection_options = \{"path": "s3://octank-energy-datalake-west/Tier-2/Customer","partitionKeys": ["customerid"]\}, format = "csv", transformation_ctx = "customerSink")\
\
print "Written the DFs - ", datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')\
\
job.commit()\
\
print "Commited the job ",datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')\
\
}