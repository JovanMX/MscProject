package uk.ac.manchester.rtccfd.spark2;

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{ RandomForestClassificationModel, RandomForestClassifier }
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ IndexToString, StringIndexer, VectorIndexer, LabeledPoint }
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.mllib.evaluation.MulticlassMetrics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.SaveMode
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.log4j.{ Level, LogManager, PropertyConfigurator }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import uk.ac.manchester.rtccfd.spark2.utils.CommonUtils
import uk.ac.manchester.rtccfd.spark2.utils.FileUtils
import org.apache.spark.sql.types.StringType

object MetricsEvaluation {
  val logger = LogManager.getRootLogger


  def main(args: Array[String]) {
    
    val directory = CommonUtils.prepareDirectoryPath(args(0))
    val localDirectory = CommonUtils.prepareDirectoryPath(args(1))

/********************************************************************
     * SPARK CONTEXT
     ********************************************************************/

    // Create the Spark Context
    val conf = new SparkConf()
      .setAppName("CCFD Metrics Evaluation");
    val sc = new SparkContext(conf);
    val sparkSession = SparkSession.builder().getOrCreate();
    import sparkSession.implicits._;
    
    val predictions = sparkSession.read
      .format("csv")
      .option("header", false)
      .option("delimiter", ",")
      .option("mode", "DROPMALFORMED")
      .schema(schema)
      .load(directory+"predictions.csv");
    
    
          //Evaluation metrics
      val metrics = new MulticlassMetrics(predictions.select("predictedLabel", "label").rdd.
        map(x =>
          
          (x.get(0).asInstanceOf[Double], x.get(1).asInstanceOf[Double])))
      
      val cfm = metrics.confusionMatrix      
      val tn = cfm(0, 0)
      val fp = cfm(0, 1)
      val fn = cfm(1, 0)
      val tp = cfm(1, 1)
      
    
    // Select (prediction, true label) and compute test error
    val binaryEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
      
      
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")


    //val rfModel = model.stages(1).asInstanceOf[RandomForestClassificationModel]
    //log.warn("Learned classification forest model:\n" + rfModel.toDebugString)

    def evaluateMetric(metricName: String) = {
      val valMetric = evaluator.setMetricName(metricName).evaluate(predictions)
      valMetric
    }
    def evaluateBinaryMetric(metricName: String) = {
      val valMetric = binaryEvaluator.setMetricName(metricName).evaluate(predictions)
      valMetric
    }
    val accuracy = evaluateMetric("accuracy")
    val areaUnderROC = evaluateBinaryMetric("areaUnderROC")
    val areaUnderPR = evaluateBinaryMetric("areaUnderPR")
    val precision = (tp / (tp + fp))
    val sensitivity = (tp / (tp + fn))
    val specificity = (fp / (fp + tn))

//    predictions.write.mode(SaveMode.Overwrite).csv(directory + "predictions_partitioned.csv")
    FileUtils.mergeFiles(sc, directory + "predictions.csv", directory + "predictions_joined.csv")

    //Create file for results if not exists
    FileUtils.createIfNotExists(sc, directory + "metric_evaluation_test.csv")
    
    val evaluationResults = new StringBuilder(tp.toString()).append(",")
        .append(fp).append(",")
        .append(fn).append(",")
        .append(tn).append(",")
        .append(accuracy).append(",")
        .append(precision).append(",")
        .append(sensitivity).append(",")
        .append(specificity).append(",")
        .append(areaUnderROC).append(",")
        .append(areaUnderPR).toString()
    FileUtils.appendLineToFile(sc, directory + "metric_evaluation_test.csv", evaluationResults)
    FileUtils.copyToLocal(sc, directory + "metric_evaluation_test.csv", localDirectory+ "metric_evaluation_test.csv")
    FileUtils.copyToLocal(sc, directory + "predictions_joined.csv", localDirectory+ "predictions_joined.csv")

//    
//    println("accuracy: "+accuracy)
//    
////    log.warn("numPos:::::::::::::::::::: = " + numPos)
//    logger.warn("Test Error = " + (1.0 - accuracy))
//    logger.warn("areaUnderROC = " + areaUnderROC)
//    logger.warn("areaUnderPR = " + areaUnderPR)
//
//            predictions.select("predictedLabel", "label", "features").show()
//    logger.warn("Num. Transactions = " + predictions.count())

  }
  
  
  // Define the CSV Dataset Schema
  val schema = new StructType(Array(
    StructField("uuid", StringType, true),
    StructField("id", LongType, true),
    StructField("prediction", DoubleType, true),
    StructField("predictedLabel", DoubleType, true),
    StructField("label", DoubleType, true),
    StructField("incoming_time", LongType, true),
    StructField("prediction_time", LongType, true)));
  
}