package uk.ac.manchester.rtccfd.spark2.utils

import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import java.net.URI
import org.apache.log4j.LogManager

object FileUtils {
  val log = LogManager.getRootLogger

  def mkDir(sc: SparkContext, path: String): Unit = {
    var fileSystem: FileSystem = null
    var conf: Configuration = null
    conf = new Configuration()
    fileSystem = FileSystem.get(conf)
    val p = new Path(path)
    val exists = fileSystem.exists(p)
    
    if(!exists){
      fileSystem.mkdirs(p)
    }
    
  }
  
  def mergeFiles(sc: SparkContext, srcPath: String, dstPath: String, s3BucketPath: String = null): Unit = {
    var fileSystem: FileSystem = null
    var conf: Configuration = null
    if (srcPath.toLowerCase().startsWith("s3")) {
      conf = sc.hadoopConfiguration
      fileSystem = FileSystem.get(new URI(s3BucketPath), conf)
    } else {
      conf = new Configuration()
      fileSystem = FileSystem.get(conf)
    }
    fileSystem.delete(new Path(dstPath))
    FileUtil.copyMerge(fileSystem, new Path(srcPath), fileSystem, new Path(dstPath), true, conf, null)
  }

  def deleteFile(sc: SparkContext, path: String, s3BucketPath: String = null): Unit = {
    var fileSystem: FileSystem = null
    var conf: Configuration = null
    if (path.toLowerCase().startsWith("s3")) {
      conf = sc.hadoopConfiguration
      fileSystem = FileSystem.get(new URI(s3BucketPath), conf)
    } else {
      conf = new Configuration()
      fileSystem = FileSystem.get(conf)
    }
    fileSystem.delete(new Path(path))
  }

  def deleteDirectory(sc: SparkContext, path: String, s3BucketPath: String = null): Unit = {
    var fileSystem: FileSystem = null
    var conf: Configuration = null
    if (path.toLowerCase().startsWith("s3")) {
      conf = sc.hadoopConfiguration
      fileSystem = FileSystem.get(new URI(s3BucketPath), conf)
    } else {
      conf = new Configuration()
      fileSystem = FileSystem.get(conf)
    }
    fileSystem.delete(new Path(path), true)
  }

  def createIfNotExists(sc: SparkContext, path: String) {

    var fileSystem: FileSystem = null
    var conf: Configuration = sc.hadoopConfiguration
    fileSystem = FileSystem.get(conf)
    val p = new Path(path);
    val exists = fileSystem.exists(p)
    
    if(!exists){
      val o = fileSystem.create(p);
      o.close()
    }
      
  }

  def appendLineToFile(sc: SparkContext, path: String, line: String) {

    var fileSystem: FileSystem = null
    var conf: Configuration = sc.hadoopConfiguration
    fileSystem = FileSystem.get(conf)
    val p = new Path(path);
    val stm = fileSystem.append(p);
    try {
      stm.write((line + "\n").getBytes("UTF-8"))
    } finally {
      stm.close()
    }
  }
  
  
  

  def copyToLocal(sc: SparkContext, path: String, destPath: String) {

    var fileSystem: FileSystem = null
    var conf: Configuration = sc.hadoopConfiguration
    fileSystem = FileSystem.get(conf)
    val p = new Path(path);
    val dest = new Path(destPath);
    
    val exists = fileSystem.exists(p)
    
    if(exists){
      fileSystem.copyToLocalFile(false, p, dest, true);
    }
      
  }

}