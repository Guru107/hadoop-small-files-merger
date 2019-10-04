package hadoop.small.files.merger.utils

import java.text.SimpleDateFormat
import java.util.Calendar

case class CommandLineArgs(directory: String = "", compression: String = "none", schemaString: String = "", schemaPath: String = "", configFile: String = "", format: String = "avro", fromDate: Calendar = null, toDate: Calendar = null, blockSize: Long = 131072000, datePartitionFormat: SimpleDateFormat = new SimpleDateFormat("'/year='yyyy'/month='MM'/day='dd"), partitionBy: String = "day")
