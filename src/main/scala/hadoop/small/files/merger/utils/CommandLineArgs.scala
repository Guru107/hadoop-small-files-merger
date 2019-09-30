package hadoop.small.files.merger.utils

import java.util.Calendar

case class CommandLineArgs(directory: String = "", schemaString: String = "", schemaPath: String = "", configFile: String = "", format: String = "avro", fromDate: Calendar = null, toDate: Calendar = null, blockSize: Long = 131072000)
