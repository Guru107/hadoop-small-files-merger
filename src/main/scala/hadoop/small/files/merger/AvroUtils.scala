package hadoop.small.files.merger

import hadoop.small.files.merger.utils.HDFSUtils
import org.apache.avro.Schema


class AvroUtils(hdfsUtils: HDFSUtils) {
  private val _schemaparser = new Schema.Parser()
  private val _hdfsUtils = hdfsUtils

  def getSchema(schemaPath: String, schemaString: String): String = {
    if (schemaPath.nonEmpty && _hdfsUtils.exists(schemaPath)) parseSchema(_hdfsUtils.getFile(schemaPath))
    else parseSchema(schemaString)
  }

  private def parseSchema(avroSchema: String): String = {
    _schemaparser.parse(avroSchema).toString
  }
}
