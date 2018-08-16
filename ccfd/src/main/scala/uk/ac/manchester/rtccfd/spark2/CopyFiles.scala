package uk.ac.manchester.rtccfd.spark2;

import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.DataFrame

import sparksmote.SMOTE.SMOTE

import uk.ac.manchester.rtccfd.spark2.utils.CommonUtils
import uk.ac.manchester.rtccfd.spark2.utils.FileUtils
import org.apache.spark.sql.types.StringType

object CopyFiles {

  def main(args: Array[String]) {

    val start = System.nanoTime()
    val log = LogManager.getRootLogger

    //read mandatory arguments

    val inputDir = CommonUtils.prepareDirectoryPath(args(0))
    val outputDirectory = CommonUtils.prepareDirectoryPath(args(1))
    
    val sparkSession = SparkSession.builder().getOrCreate();
    val metrics = sparkSession.read
      .format("csv")
      .option("header", false)
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .schema(tuningMetrics)
      .load(inputDir + "metric_evaluation_tuning.csv");
    val submodels = sparkSession.read
      .format("csv")
      .option("header", false)
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .schema(submodelsSchema)
      .load(inputDir + "metric_evaluation_submodels_tuning.csv");
    val test = sparkSession.read
      .format("csv")
      .option("header", false)
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(inputDir + "creditcard_test_partition.csv");
    
    metrics.coalesce(1).write.mode(SaveMode.Overwrite).csv(outputDirectory+ "creditcard_test_partition.csv")
    metrics.coalesce(1).write.mode(SaveMode.Append).csv(outputDirectory+ "metric_evaluation_tuning.csv")
    metrics.coalesce(1).write.mode(SaveMode.Append).csv(outputDirectory+ "metric_evaluation_submodels_tuning.csv")
    
  }
  
  
      

  val submodelsSchema = new StructType(Array(
    StructField("executionTime", StringType, true),
    StructField("undersampling", DoubleType, true),
    StructField("oversampling", LongType, true),
    StructField("params", StringType, true),
    StructField("metric", DoubleType, true)));
      
      
      
  // Define the CSV Dataset Schema
  val schema = new StructType(Array(
    StructField("V1", DoubleType, true),
    StructField("V2", DoubleType, true),
    StructField("V3", DoubleType, true),
    StructField("V4", DoubleType, true),
    StructField("V5", DoubleType, true),
    StructField("V6", DoubleType, true),
    StructField("V7", DoubleType, true),
    StructField("V8", DoubleType, true),
    StructField("V9", DoubleType, true),
    StructField("V10", DoubleType, true),
    StructField("V11", DoubleType, true),
    StructField("V12", DoubleType, true),
    StructField("V13", DoubleType, true),
    StructField("V14", DoubleType, true),
    StructField("V15", DoubleType, true),
    StructField("V16", DoubleType, true),
    StructField("V17", DoubleType, true),
    StructField("V18", DoubleType, true),
    StructField("V19", DoubleType, true),
    StructField("V20", DoubleType, true),
    StructField("V21", DoubleType, true),
    StructField("V22", DoubleType, true),
    StructField("V23", DoubleType, true),
    StructField("V24", DoubleType, true),
    StructField("V25", DoubleType, true),
    StructField("V26", DoubleType, true),
    StructField("V27", DoubleType, true),
    StructField("V28", DoubleType, true),
    StructField("Amount", DoubleType, true),
    StructField("Class", IntegerType, false)));
      
      
      
      
      
  val tuningMetrics = new StructType(Array(
    StructField("executionTime", StringType, true),
    StructField("numWorkerNodes", LongType, true),
    StructField("undersampling", DoubleType, true),
    StructField("oversampling", LongType, true),
    StructField("numTrees", LongType, true),
    StructField("maxDepth", LongType, true),
    StructField("maxBins", LongType, true),
    StructField("impurity", StringType, true),
    StructField("featureSubsetStrategy", StringType, true),
    StructField("minInstancesPerNode", LongType, true),
    StructField("tp", DoubleType, true),
    StructField("fp", DoubleType, true),
    StructField("fn", DoubleType, true),
    StructField("tn", DoubleType, true),
    StructField("accuracy", DoubleType, true),
      StructField("precision", DoubleType, true),
      StructField("sensitivity", DoubleType, true),
      StructField("specificity", DoubleType, true),
      StructField("areaUnderROC", DoubleType, true),
      StructField("areaUnderPR", DoubleType, true),
      StructField("areaunderPRCV", DoubleType, true),
      StructField("param2", StringType, true),
      StructField("param3", StringType, true),
      StructField("param7", StringType, true),
      StructField("param8", StringType, true),
      StructField("param9", StringType, true),
      StructField("param10", StringType, true),
      StructField("param11", StringType, true),
      StructField("param12", StringType, true),
      StructField("param13", StringType, true),
      StructField("param14", StringType, true),
      StructField("param15", StringType, true)));
}
