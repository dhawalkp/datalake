{\rtf1\ansi\ansicpg1252\cocoartf1504\cocoasubrtf830
{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww10800\viewh8400\viewkind0
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
## @args: [database = "employees", table_name = "tier1_bucket", transformation_ctx = "datasource0"]\
## @return: datasource0\
## @inputs: []\
employeeFullDF = glueContext.create_dynamic_frame.from_catalog(database = "employee-tier1", table_name = "full", transformation_ctx = "employeeFullDF")\
\
print "Creating DFs - ",datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')\
\
## @type: ApplyMapping\
## @args: [mapping = [("op", "string", "op", "string"), ("employee_id", "long", "employee_id", "long"), ("first_name", "string", "first_name", "string"), ("last_name", "string", "last_name", "string"), ("email", "string", "email", "string"), ("phone_number", "string", "phone_number", "string"), ("hire_date", "string", "hire_date", "string"), ("job_id", "string", "job_id", "string"), ("salary", "double", "salary", "double"), ("commission_pct", "double", "commission_pct", "double"), ("manager_id", "long", "manager_id", "long"), ("department_id", "long", "department_id", "long"), ("partition_0", "string", "partition_0", "string"), ("partition_1", "string", "partition_1", "string")], transformation_ctx = "applymapping1"]\
## @return: applymapping1\
## @inputs: [frame = datasource0]\
applymapping1 = ApplyMapping.apply(frame = employeeFullDF, mappings = [ ("employee_id", "long", "employee_id", "long"), ("first_name", "string", "first_name", "string"), ("last_name", "string", "last_name", "string"), ("email", "string", "email", "string"), ("phone_number", "string", "phone_number", "string"), ("hire_date", "string", "hire_date", "string"), ("job_id", "string", "job_id", "string"), ("salary", "double", "salary", "double"), ("commission_pct", "double", "commission_pct", "double"), ("manager_id", "long", "manager_id", "long"), ("department_id", "long", "department_id", "long")], transformation_ctx = "applymapping1")\
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
employeeSink = glueContext.write_dynamic_frame.from_options(frame = partitionedFinalDF, connection_type = "s3", connection_options = \{"path": "s3://employee-bucket-dev/Tier-2","partitionKeys": ["employee_id"]\}, format = "parquet", transformation_ctx = "employeeSink")\
\
print "Written the DFs - ", datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')\
\
job.commit()\
\
print "Commited the job ",datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')\
\
}