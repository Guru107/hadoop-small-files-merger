package hadoop.small.files.merger

import java.util.Calendar

import com.databricks.spark.avro._
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

    val partitionBy = if(_arguments.partitionBy.equalsIgnoreCase("day")) 86400000 else 3600000

    if (_arguments.fromDate != null && _arguments.toDate != null && _arguments.directory.nonEmpty) {
      (_arguments.fromDate.getTimeInMillis until _arguments.toDate.getTimeInMillis)
        .by(partitionBy) //Increment by a day
        .foreach(epoch => {
          val date = Calendar.getInstance()
          date.setTimeInMillis(epoch)
          val fullDirectoryPath = s"${_arguments.directory}${_arguments.datePartitionFormat.format(date.getTime)}"
          if (_hdfsUtils.exists(fullDirectoryPath)) {
            val partitionSize = getPartitionSize(fullDirectoryPath)
            _arguments.format match {
              case "avro" => {
                mergeAvroDirectory(fullDirectoryPath, schema, partitionSize)
              }
              case "text" => {
                mergeTextDirectory(fullDirectoryPath, partitionSize)
              }
            }
            cleanUpOldDirectory(fullDirectoryPath)
          }
        })
    } else {
      val partitionSize = getPartitionSize(_arguments.directory)
      _arguments.format match {
        case "avro" => mergeAvroDirectory(_arguments.directory, schema, partitionSize)
        case "text" => mergeTextDirectory(_arguments.directory, partitionSize)
      }
    }
  }

  private def mergeAvroDirectory(directoryPath: String, schema: String, partitionSize: Int): Unit = {
    _sparkSession
      .read
      .option("avroSchema", schema)
      .avro(directoryPath)
      .repartition(partitionSize)
      .write
      .avro(s"${directoryPath}_merged")
  }

  private def mergeTextDirectory(directoryPath: String, partitionSize: Int): Unit = {
    _sparkSession
      .read
      .text(directoryPath)
      .repartition(partitionSize)
      .write
      .text(s"${directoryPath}_merged")
  }

  private def getPartitionSize(directoryPath: String): Int = {
    val directorySize = _hdfsUtils.getDirectorySize(directoryPath)
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
}
