# DataLake Formation in AWS

## High Level Data Lake Architecture

![](https://github.com/dhawalkp/datalake/blob/master/High_Level_architecture_DataLake.png)

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


