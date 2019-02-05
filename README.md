# DataLake Formation in AWS

A Data lake contains all data, both raw sources over extended periods of time as well as any processed data. They enable users across multiple business units to refine, explore and enrich data on their terms. Also, enables multiple data access patterns across a shared infrastructure: batch, interactive, online, search, in-memory and other processing engines.

AWS Glue provides a serverless Spark-based data processing service. Glue is based on open source frameworks like Apache Spark and the Hive Metastore. This allows our users to go beyond the traditional ETL use cases into more data prep and data processing spanning data exploration, data science, and ofcourse data prep for analytics. 
Glue helps users do three things – Discover and understand their data by cataloguing it into a central metadata Catalog, offering libraries and tools to efficiently develop their data prep code, and then run it at scale on a serverless, full managed environment. 

The backbone storage service for Data Lake best suited is AWS S3. The native features of S3 are exactly what you want from a Data Lake - Replication across AZ’s for high availability and durability, Massively parallel, scalable (Storage scales independent of compute) and Low storage cost.

## High Level Data Lake Architecture

![](https://github.com/dhawalkp/datalake/blob/master/High_Level_architecture_DataLake.png)

# Building a Data Lake from a Relational Data Store

This repository contains the sample reference implementation to create data lake from Relational Database services as one of the sources.

* Source RDS Employee Table Schema:
EMPLOYEE_ID	FIRST_NAME	LAST_NAME	EMAIL	PHONE_NUMBER	HIRE_DATE	JOB_ID	SALARY	COMMISSION_PCT	MANAGER_ID	DEPARTMENT_ID

* Sample Full Log Employee record format generated by DMS:
EMPLOYEE_ID	FIRST_NAME	LAST_NAME	EMAIL	PHONE_NUMBER	HIRE_DATE	JOB_ID	SALARY	COMMISSION_PCT	MANAGER_ID	DEPARTMENT_ID
321	William115	Gietzupd2	WGIETZ115	515.123.8181	6/7/94 0:00	AC_ACCOUNT	8300		205	110

* Sample CDC Log Employee Record format generated by DMS:
Op	EMPLOYEE_ID	FIRST_NAME	LAST_NAME	EMAIL	PHONE_NUMBER	HIRE_DATE	JOB_ID	SALARY	COMMISSION_PCT	MANAGER_ID	DEPARTMENT_ID
U	321	William115	Gietzupd2	WGIETZ115	515.123.8181	6/7/94 0:00	AC_ACCOUNT	8300		205	110

Please notice the additional OP column in CDC log file added by DMS to tag the type of change.

The implementation consists of 
## Hydrating the Data Lake
* AWS RDS backed by Oracle DB engine integrating with AWS DMS service generating Full and CDC Log files and storing the files on Tier-1 S3 Bucket. Alternatively, the Tier-1 bucket can be hydrated by periodic export process that dumps all the changes as well.
* The Tier-1 bucket in this example is partitioned based on year/month/day/hour
## Creating Glue Data Catalog of Tier-1 Bucket for processing
* AWS Glue Crawlers needs to be configured in order to process CDC and Full Log files in the tier-1 bucket and create data catalog for both. In this case, the Tier-1 Database in Glue will consist of 2 tables i.e. CDC and Full

## Glue ETL Job for Tier-2 Data
* Tier-2 ETL job will re-partition based on required keys and hydrate the tier-2 buckets in S3 with S3 Objects based on the partition keys. The script is available in this repo with name - DMS_CDC_Crawler.py and DMS_Full_Crawler.py
* The Tier-2 S3 bucket will eventually have Partitions consisting of multiple versions of S3 Objects for same key.

## Compaction ETL Job
* The Compaction job i.e. Compaction_Job.py basically deletes the older versions of Objects for all the partitions in tier-2 bucket. In this example, the script uses the last modified time stamp of S3 Object to build the compaction logic. Please refer to additional considerations section below in this documentation for alternative approaches to preserve the consistency.


## Detailed Data Lake Formation Architecture

![](https://github.com/dhawalkp/datalake/blob/master/Data_Pipeline_Architecture.png)

## Best Practices and Performance Considerations
* One of the key design decisions in making the solution performant will be the selection of appropriate partition keys for target S3 buckets. Please read [Working with Partitioned Data in AWS Glue](https://aws.amazon.com/blogs/big-data/work-with-partitioned-data-in-aws-glue/). Partitioning the Source and Target Buckets via Relevant Partition Keys and making use of it in avoiding cross partition joins or full scans.

* Use Parquet or ORC and S3 Paths for formatting and organizing the data as per partition keys with Compression like Snappy/Gzip formats
* Use Glue DynamicFrames and make use of PushDownPRedicates based on Partitions to improve/reduce on GETs/PUTs to S3
* Use applyMapping wherever possible to restrict the columns
* In Writing the dynamic frames into partitioned sinks, try to use additional Partitionkeys option so that you can directly write it from DynamicFrame instead of doing intermediate conversion into Spark DataFrame.
* Use Compaction Technique periodically to delete the old objects for the same key.
* DMS CDC/Full Load files contains timestamp in the name and should be used to process the data based on this. Care must be taken to make sure multiple CDC Records for same key are not processed in parallel to avoid Data Consistency issues during CDC records consolidation phase.

## Assumptions & Additional Considerations

### Consistency and Ordering
* Multiple changes on the same record can appear in a CDC file generated by AWS DMS.Though, CDC guarantees the files delivered to S3 are in order. The transaction integrity can be maintained as long as you follow the ordering of changes in the file and across sequence of files. The recommendation will be to also have Modification Time Stamp embedded in the source table so that the consistency can be maintained. The Alternative approach to not having modification time stamp in the source table will be to implement pre-tier-1 ETL job that transforms the CDC records and add a synthetic sequence number.

* Since S3 updates are eventual consistent, a mechanism needs to be developed to make sure the data being queried are not being mutated at the same time. You have to employ the use of temporary scratch location and then compact in tier-3 bucket for example.
* There may be delays in S3 Upserts and so reliance on S3 Object Last Modified Time may cause inconsistent results. An alternative approach could be to use the DMS CDC logtimestamp or Update Time Stamp column (if present) of the original data and add as meta data in S3 Object and use that for compaction job.
### Idempotence

* The ETL job should be aware of idempotence requirements. Since CDC Records presents the latest copy of entire record, the idempotence should not be a concern for ETL job in this case.
### Performance Tuning the DMS

* The solution must be aware of the performance tuning and limits of DMS service as mentioned here -https://docs.aws.amazon.com/dms/latest/userguide/CHAP_BestPractices.html

