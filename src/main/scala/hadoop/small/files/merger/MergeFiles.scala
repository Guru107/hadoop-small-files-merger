package hadoop.small.files.merger

import java.util.Calendar


import hadoop.small.files.merger.utils.{CommandLineArgs, HDFSUtils}
import org.apache.spark.sql.SparkSession


class MergeFiles(sparkSession: SparkSession, commandLineArgs: CommandLineArgs) {
  private val _sparkSession = sparkSession
  private val _arguments = commandLineArgs
  private val _hdfsUtils = new HDFSUtils

  def start(): Unit = {
    mergeFiles()
  }

  private def mergeFiles(): Unit = {
    val schema = if (_arguments.format.equalsIgnoreCase("avro")) {
      val avroUtils = new AvroUtils(_hdfsUtils)
      avroUtils.getSchema(_arguments.schemaPath, _arguments.schemaString)
    } else ""

    val partitionBy = if (_arguments.partitionBy.equalsIgnoreCase("day")) 86400000 else 3600000

    if (_arguments.fromDate != null && _arguments.toDate != null && _arguments.directory.nonEmpty) {
      (_arguments.fromDate.getTimeInMillis until _arguments.toDate.getTimeInMillis)
        .by(partitionBy)
        .foreach(epoch => {
          val date = Calendar.getInstance()
          date.setTimeInMillis(epoch)
          val fullDirectoryPath = s"${_arguments.directory}${_arguments.datePartitionFormat.format(date.getTime)}"

          if (_hdfsUtils.exists(fullDirectoryPath)) {
            val partitionSize = getPartitionSize(fullDirectoryPath)
            println(s"Current Directory: $fullDirectoryPath")
            println(s"Number of write partitions: ${partitionSize}")
            _arguments.format match {
              case "avro" => {
                mergeAvroDirectory(fullDirectoryPath, schema, partitionSize)
              }
              case "text" => {
                mergeTextDirectory(fullDirectoryPath, partitionSize)
              }
              case "parquet" => {
                mergeParquetDirectory(fullDirectoryPath, partitionSize)
              }
            }

          }
        })
    } else {
      val partitionSize = getPartitionSize(_arguments.directory)
      println(s"Number of write partitions: ${partitionSize}")
      _arguments.format match {
        case "avro" => mergeAvroDirectory(_arguments.directory, schema, partitionSize)
        case "text" => mergeTextDirectory(_arguments.directory, partitionSize)
        case "parquet" => mergeParquetDirectory(_arguments.directory, partitionSize)
      }
    }
  }

  private def mergeAvroDirectory(directoryPath: String, schema: String, partitionSize: Int): Unit = {
    try{
      _sparkSession
        .read
        .option("avroSchema", schema)
        .format("avro")
        .load(directoryPath)
        .repartition(partitionSize)
        .write
        .option("compression", _arguments.compression)
        .format("avro")
        .save(s"${directoryPath}_merged")

      cleanUpOldDirectory(directoryPath)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }

  }

  private def cleanUpOldDirectory(directoryPath: String): Unit = {
    if (_hdfsUtils.renameDir(directoryPath, s"${directoryPath}_bak")) {
      println("Source Directory renamed")
      if (_hdfsUtils.renameDir(s"${directoryPath}_merged", directoryPath)) {
        println("Merged Directory renamed")
        if (_hdfsUtils.moveToTrash(s"${directoryPath}_bak")) {
          println(s"Moved ${directoryPath}_bak to trash")
        }
      }
    }
  }

  private def mergeTextDirectory(directoryPath: String, partitionSize: Int): Unit = {
    _sparkSession
      .read
      .text(directoryPath)
      .repartition(partitionSize)
      .write
      .option("compression", _arguments.compression)
      .text(s"${directoryPath}_merged")
    cleanUpOldDirectory(directoryPath)
  }

  private def mergeParquetDirectory(directoryPath: String, partitionSize: Int): Unit = {
    _sparkSession
      .read
      .parquet(directoryPath)
      .repartition(partitionSize)
      .write
      .option("compression", _arguments.compression)
      .parquet(s"${directoryPath}_merged")
    cleanUpOldDirectory(directoryPath)
  }

  private def getPartitionSize(directoryPath: String): Int = {
    val directorySize = _hdfsUtils.getDirectorySize(directoryPath)
    println(s"Directory Size In Bytes: ${directorySize}")
    val blockSize = commandLineArgs.blockSize
    val partitionSize = {
      if (directorySize <= blockSize) 1
      else {
        val divFactor = directorySize / blockSize
        val ceilDivFactor = Math.ceil(directorySize / blockSize).toInt
        if (divFactor > ceilDivFactor) {
          ceilDivFactor + 1
        } else ceilDivFactor
      }
    }
    partitionSize
  }
}
