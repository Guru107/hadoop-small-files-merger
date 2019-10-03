# hadoop-small-files-merger
A Spark application to merge small files

```
Hadoop Small Files Merger Application
Usage: hadoop-small-files-merger.jar [options]

  -b, --blockSize <value>  Specify your clusters blockSize in bytes, Default is set at 131072000
  -f, --format Values: avro,text
                           
  -d, --directory <value>  Starting with hdfs:///
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
