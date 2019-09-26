package hadoop.small.files.merger.utils

import java.util.Calendar

import scopt.OptionParser

class CommandLineArgsParser(hdfsUtils: HDFSUtils) {
  val parser = new OptionParser[CommandLineArgs]("hadoop-small-files-merger.jar") {
    head("Hadoop Small Files Merger Application")

    opt[Long]('b', "blockSize")
      .optional()
      .text("Specify your clusters blockSize in bytes, Default is set at 131072000")
      .action((blockSize, commandLineOpts) => commandLineOpts.copy(blockSize = blockSize))

    opt[String]('f', "format")
      .required()
      .valueName("Values: avro")
      .action((format, commandLineOpts) => commandLineOpts.copy(format = format))
      .children(
        opt[String]('d', "directory")
          .text("Starting with hdfs:///")
          .optional()
          .action((directory, commandLineArgs) => commandLineArgs.copy(directory)),
        opt[String]('s', "schemaStr")
          .text("A stringified avro schema")
          .optional()
          .action((schemaStr, commandLineArgs) => commandLineArgs.copy(schemaString = schemaStr)),
        opt[String]('s', "schemaPath")
          .optional()
          .text("HDFS Path to .avsc file.")
          .action((schemaPath, commandLineArgs) => commandLineArgs.copy(schemaPath = schemaPath)),
        note("\nSpecify either `schemaStr` or `schemaPath`\n"),
        //          opt[String]('c',"configFile")
        //            .text("A .properties file in HDFS with `directory` and (`schemaStr` or `schemaPath`) properties")
        //            .optional()
        //            .action((configFile,commandLineArgs)=>commandLineArgs.copy(configFile = configFile)),

        note("\nBelow options work if directory is partitioned by date inside `directory` as, `directory`/year=<year>/month=<month>/day=<day>\n"),
        opt[Calendar]("from")
          .text("From Date")
          .action((from, commandLineArgs) => commandLineArgs.copy(fromDate = from)),

        opt[Calendar]("to")
          .text("To Date")
          .action((to, commandLineArgs) => commandLineArgs.copy(toDate = to)),

        checkConfig(commandLine => {
          if (commandLine.schemaPath.nonEmpty && commandLine.schemaString.nonEmpty) failure("Specifiy either `schmeaStr` or `schemaPath`") else success
        })
      )

  }

  def getSchemaString(commandLineArgs: CommandLineArgs): String = {
    if (commandLineArgs.schemaPath.nonEmpty) hdfsUtils.getFile(commandLineArgs.schemaPath)
    else commandLineArgs.schemaString
  }

  def getDatePartitionedPath(commandLineArgs: CommandLineArgs, date: Calendar): String = {
    s"${commandLineArgs.directory}/year=${date.get(Calendar.YEAR)}/month=${"%02d".format(date.get(Calendar.MONTH) + 1)}/day=${"%02d".format(date.get(Calendar.DAY_OF_MONTH))}"
  }
}
