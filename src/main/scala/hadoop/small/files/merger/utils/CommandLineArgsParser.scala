package hadoop.small.files.merger.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import scopt.OptionParser

class CommandLineArgsParser {
  val parser = new OptionParser[CommandLineArgs]("hadoop-small-files-merger.jar") {
    head("Hadoop Small Files Merger Application")

    opt[Long]('b', "blockSize")
      .optional()
      .text("Specify your clusters blockSize in bytes, Default is set at 132120576")
      .action((blockSize, commandLineOpts) => commandLineOpts.copy(blockSize = blockSize))

    opt[String]('f', "format")
      .required()
      .valueName("Values: avro")
      .action((format, commandLineOpts) => commandLineOpts.copy(format = format))
      .children(
        opt[String]('d', "directory")
          .text("Starting with hdfs:///")
          .required()
          .action((directory, commandLineArgs) => commandLineArgs.copy(directory)),
        opt[String]('s', "schemaStr")
          .text("A stringified avro schema")
          .optional()
          .action((schemaStr, commandLineArgs) => commandLineArgs.copy(schemaString = schemaStr)),
        opt[String]('s', "schemaPath")
          .optional()
          .text("HDFS Path to .avsc file. if format specified is avro")
          .action((schemaPath, commandLineArgs) => commandLineArgs.copy(schemaPath = schemaPath)),
        note("\nSpecify either `schemaStr` or `schemaPath`\n"),


        note("\nBelow options work if directory is partitioned by date inside `directory`\n"),
        opt[Calendar]("from")
          .optional()
          .text("From Date")
          .action((from, commandLineArgs) => commandLineArgs.copy(fromDate = from)),

        opt[Calendar]("to")
          .optional()
          .text("To Date")
          .action((to, commandLineArgs) => commandLineArgs.copy(toDate = to)),
        opt[String]("partitionBy")
          .optional()
          .valueName("Values: day or hour")
          .text("Directory partitioned by. Default: day")
          .action((partitionBy, commandLineArgs) => commandLineArgs.copy(partitionBy = partitionBy)),
        opt[String]("partitionFormat")
          .optional()
          .text("Give directory partition format using valid SimpleDateFormat pattern Eg. \"'/year='yyyy'/month='MM'/day='dd\", \"'/'yyyy'/'MM'/'dd\"")
          .validate(partitionFormat => {
            try {
              new SimpleDateFormat(partitionFormat)
              success
            } catch {
              case e: Exception => failure(e.getMessage)
            }
          }).action((fmt, commandLineArgs) => commandLineArgs.copy(datePartitionFormat = new SimpleDateFormat(fmt))),

        checkConfig(commandLine => {
          if (commandLine.format.contentEquals("avro") && commandLine.schemaPath.length == 0 && commandLine.schemaString.length == 0) failure("Specifiy either `schmeaStr` or `schemaPath`") else success
        }),
        checkConfig(commandLine => {
          if (commandLine.fromDate == null &&  commandLine.toDate != null || commandLine.toDate == null && commandLine.fromDate != null)
            failure("Specify `fromDate` and `toDate` both or neither of them")
          else success
        })
      )

  }
}
