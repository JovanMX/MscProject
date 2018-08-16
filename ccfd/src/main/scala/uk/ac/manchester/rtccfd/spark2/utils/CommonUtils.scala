package uk.ac.manchester.rtccfd.spark2.utils

import org.apache.spark.SparkContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import java.net.URI
import org.apache.log4j.LogManager

object CommonUtils {
  val log = LogManager.getRootLogger


  def prepareDirectoryPath(path: String): String = {
    if(path.endsWith("/"))
      path
    else
      path+"/"
  }

    

    
}