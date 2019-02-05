import sys
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
botoClient = boto3.client('glue',region_name='us-west-2')
## @type: DataSource
## @args: [database = "employee-tier1", table_name = "cdc", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
## Dynamic Frame for the CDC Tier1 Records
pushDownPredicate = "(year=='2019' and month=='01' and day=='03')"

employeeCDCDF = glueContext.create_dynamic_frame.from_catalog(database = "employee_tier1", table_name = "cdc",push_down_predicate=pushDownPredicate, transformation_ctx = "employeeCDCDF")


## @type: ApplyMapping
## @args: [mapping = [("op", "string", "op", "string"), ("employee_id", "long", "employee_id", "long"), ("first_name", "string", "first_name", "string"), ("last_name", "string", "last_name", "string"), ("email", "string", "email", "string"), ("phone_number", "string", "phone_number", "string"), ("hire_date", "string", "hire_date", "string"), ("job_id", "string", "job_id", "string"), ("salary", "double", "salary", "double"), ("commission_pct", "string", "commission_pct", "string"), ("manager_id", "long", "manager_id", "long"), ("department_id", "long", "department_id", "long"), ("year", "string", "year", "string"), ("month", "string", "month", "string"), ("day", "string", "day", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping1 = ApplyMapping.apply(frame = employeeCDCDF, mappings = [("op", "string", "op", "string"), ("employee_id", "long", "employee_id", "long"), ("first_name", "string", "first_name", "string"), ("last_name", "string", "last_name", "string"), ("email", "string", "email", "string"), ("phone_number", "string", "phone_number", "string"), ("hire_date", "string", "hire_date", "string"), ("job_id", "string", "job_id", "string"), ("salary", "double", "salary", "double"), ("commission_pct", "string", "commission_pct", "string"), ("manager_id", "long", "manager_id", "long"), ("department_id", "long", "department_id", "long"), ("year", "string", "year", "string"), ("month", "string", "month", "string"), ("day", "string", "day", "string")], transformation_ctx = "applymapping1")

sparkDF = applymapping1.toDF();


for employeeInstance in sparkDF.rdd.collect():
    print "Processing started for RDDs.."
    if employeeInstance["op"] == "U":
        print "Processing Update record..",employeeInstance["employee_id"]
        pushDownEmployeeIdPredicate = "(employee_id=="+str(employeeInstance["employee_id"])+")"
        employeeFinalDF = glueContext.create_dynamic_frame.from_catalog(database = "employee-tier2", table_name = "tier_2", push_down_predicate=pushDownEmployeeIdPredicate, transformation_ctx = "employeeFinalDF")
        if employeeFinalDF.count() > 0:
        ##try:
        ##    response = botoClient.delete_partition(
        ##    DatabaseName='employee-tier2',
        ##    TableName='tier_2',
        ##        PartitionValues=[
        ##            str(employeeInstance["employee_id"]
        ##        ]
        ##    )
        ##    print "Deleted the Partition ",str(employeeInstance["employee_id"])
        ##except:
        ##    print "Partition/Key not found with Key as ", str(employeeInstance["employee_id"])
            
            ##//Refreshing the Dataframe after deleting
            ##employeeFinalDF = glueContext.create_dynamic_frame.from_catalog(database = "employee-tier2", table_name = "tier_2", push_down_predicate=pushDownEmployeeIdPredicate, transformation_ctx = "employeeFinalDF")
        
            tempDF = spark.createDataFrame([employeeInstance])
            columns_to_drop = ['op','year', 'month','day']
            tempDF=tempDF.drop(*columns_to_drop)
            tempDynF = DynamicFrame.fromDF(tempDF,glueContext,"tempDynF")
        
           
            employeeUpdateSinkDF = glueContext.write_dynamic_frame.from_options(frame = tempDynF, connection_type = "s3", connection_options = {"path": "s3://employee-bucket-dev/Tier-2","partitionKeys": ["employee_id"]}, format = "parquet", transformation_ctx = "employeeUpdateSinkDF")
            print "Updated the record ", employeeInstance["employee_id"]
        

job.commit()
