# hadoop-small-files-merger
A Spark application to merge small files

```
Hadoop Small Files Merger Application
Usage: hadoop-small-files-merger.jar [options]

  -b, --blockSize <value>  Specify your clusters blockSize in bytes, Default is set at 131072000 (125MB) which is slightly less than actual 128MB block size. It is intentionally kept at 125MB to fit the data of the single partition into a block of 128MB. As spark does not create exact file sizes after partitioning but will always be approximately equal to the specified block size.
  
  -f, --format Values: avro,text
                           
  -d, --directory <value>  Starting with hdfs:///
  -c, --compression Values: `none`, `snappy`, `gzip`, and `lzo`. Default: none
                           Compression for the merged files
  -s, --schemaStr <value>  A stringified avro schema
  -s, --schemaPath <value>
                           HDFS Path to .avsc file. if format specified is avro

Specify either `schemaStr` or `schemaPath`


Below options work if directory is partitioned by date inside `directory`

  --from <value>           From Date
  --to <value>             To Date
  --partitionBy Values: day or hour
                           Directory partitioned by. Default: day
  --partitionFormat <value>
                           Give directory partition format using valid SimpleDateFormat pattern Eg. "'/year='yyyy'/month='MM'/day='dd", "'/'yyyy'/'MM'/'dd"
                           
```
