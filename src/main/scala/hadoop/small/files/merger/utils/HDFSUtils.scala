package hadoop.small.files.merger.utils

import java.io.{FileNotFoundException, StringWriter}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path, Trash}


class HDFSUtils() {
  private val hdfsConfig: Configuration = new Configuration()
  hdfsConfig.addResource(getClass.getResource("/core-site.xml"))
  hdfsConfig.addResource(getClass.getResource("/hdfs-site.xml"))
  private val fs: FileSystem = FileSystem.get(hdfsConfig)
  private val trash = new Trash(hdfsConfig)


  /**
   *
   * @return HDFS Directory size in bytes without replication
   */
  def getDirectorySize(directoryPath: String): Long = {
    val path: Path = new Path(directoryPath)
    fs.getContentSummary(path).getLength
  }

  /**
   *
   * @return HDFS Default block size, as specified in hdfs-site.xml
   */
  def getBlockSize(): Long = {
    fs.getDefaultBlockSize(fs.getHomeDirectory)
  }


  def getFile(filePath: String): String = {
    val path = new Path(filePath)
    var dis: FSDataInputStream = null
    try {
      if (fs.exists(path) && fs.isFile(path)) {
        dis = fs.open(path)
        val stringWriter = new StringWriter()
        IOUtils.copy(dis, stringWriter, "UTF-8")
        val raw = stringWriter.toString
        raw
      } else {
        throw new FileNotFoundException()
      }
    } catch {
      case e: Exception => throw e
    } finally {
      if (dis != null) dis.close()
    }

  }

  def exists(filePath: String): Boolean = {
    val path = new Path(filePath)
    fs.exists(path)
  }

  def renameDir(existingName: String, newName: String): Boolean = {
    val existingPath = new Path(existingName)
    val finalName = new Path(newName)
    fs.rename(existingPath, finalName)
  }

  def moveToTrash(directoryName: String): Boolean = {
    val path = new Path(directoryName)
    trash.moveToTrash(path)
  }

  def close(): Unit = {
    fs.close()
  }
}
