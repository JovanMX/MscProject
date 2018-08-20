package uk.ac.manchester.rtccfd.spark2;

import java.util.HashMap;
import java.util.Properties;

import kafka.serializer.DefaultDecoder;
import kafka.serializer.StringDecoder;
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.BytesDeserializer
import org.apache.kafka.common.utils.Bytes
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.rdd.RDD;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.types._;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
//import com.twitter.bijection.Injection;
//import com.twitter.bijection.avro.GenericAvroCodecs;
import org.slf4j.LoggerFactory
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import scala.io.{ Source }
import java.io.FileWriter
import org.apache.spark.sql.SaveMode
import uk.ac.manchester.rtccfd.spark2.utils.CommonUtils
import org.apache.spark.streaming.Milliseconds
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.avro.io.DatumReader
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.RowFactory
import com.databricks.spark.avro.SchemaConverters
import org.apache.spark.sql.Encoders
import uk.ac.manchester.rtccfd.spark2.utils.FileUtils
import org.apache.spark.sql.streaming.Trigger

object StructuredStreamingFraudDetection {

  val log = LoggerFactory.getLogger(getClass)

  /**
   * Consume and deserialise Avro messages from the Kafka Topics using a Direct Stream Approach.
   *
   */

  def main(args: Array[String]) = {

    if (args.length < 3) {
      System.err.println("Usage: KafkaStreamingSentimentClassifier <brokers> <topics> <outputDir>");
      System.exit(1);
    }

    val brokers = args(0)
    val topics = args(1)
    val modelDirerctory = CommonUtils.prepareDirectoryPath(args(2))
    val outputDirectory = CommonUtils.prepareDirectoryPath(args(3))
    val batchDurationQuantity = args(4).toInt
    val batchDurationUnit = args(5)
    val withSeconds = if (args.length < 7 || args(6)!="1") false else true

    val batchDuration : Long= if (batchDurationUnit == "ms") batchDurationQuantity else batchDurationQuantity*1000
    val parser = new Schema.Parser();
    val schema = parser.parse(schemaJsonWithSecond);
//    val schema = parser.parse(if(withSeconds)schemaJsonWithSecond else schemaJson);
    val strType: StructType = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
    val sparkSession = SparkSession.builder().getOrCreate();
    val sc = sparkSession.sparkContext
//    FileUtils.mkDir(sc, outputDirectory + "predictions")

    // Load the Trained Model
    val model = CrossValidatorModel.read.load(modelDirerctory + "randomForestClassifier");
    val assembler = if (withSeconds) new VectorAssembler().setInputCols(Array("NumericSecondDay", "V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10", "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18", "V19", "V20", "V21", "V22", "V23", "V24", "V25", "V26", "V27", "V28", "Amount")).setOutputCol("features")
    else new VectorAssembler().setInputCols(Array("V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10", "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18", "V19", "V20", "V21", "V22", "V23", "V24", "V25", "V26", "V27", "V28", "Amount")).setOutputCol("features")

/********************************************************************
     * KAFKA CONSUMER Structured streaming
     ********************************************************************/
    
//    
//    val u = if (withSeconds) 
//      udf((bytes: Array[Byte]) => udfWithSeconds(_), strType)
//    else
//      udf((bytes: Array[Byte]) => udfWithoutSeconds(_), strType)
    
    
    val u = if (withSeconds) 
      udf((bytes: Array[Byte]) => {
      val parser = new Schema.Parser();
      val schema = parser.parse(schemaJsonWithSecond);
      val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
      val decoder: Decoder = DecoderFactory.get().binaryDecoder(bytes, null)
      val record: GenericRecord = reader.read(null, decoder)

      Row(
        record.get("uuid").toString(),
        record.get("id"),
        record.get("NumericSecondDay"),
        record.get("V1"),
        record.get("V2"),
        record.get("V3"),
        record.get("V4"),
        record.get("V5"),
        record.get("V6"),
        record.get("V7"),
        record.get("V8"),
        record.get("V9"),
        record.get("V10"),
        record.get("V11"),
        record.get("V12"),
        record.get("V13"),
        record.get("V14"),
        record.get("V15"),
        record.get("V16"),
        record.get("V17"),
        record.get("V18"),
        record.get("V19"),
        record.get("V20"),
        record.get("V21"),
        record.get("V22"),
        record.get("V23"),
        record.get("V24"),
        record.get("V25"),
        record.get("V26"),
        record.get("V27"),
        record.get("V28"),
        record.get("Amount"),
        record.get("Class"))

    }, strType)
    else
      udf((bytes: Array[Byte]) => {
        val parser = new Schema.Parser();
        val schema = parser.parse(schemaJsonWithSecond);
        val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
        val decoder: Decoder = DecoderFactory.get().binaryDecoder(bytes, null)
        val record: GenericRecord = reader.read(null, decoder)

        Row(
          record.get("uuid").toString(),
          record.get("id"),
          record.get("V1"),
          record.get("V2"),
          record.get("V3"),
          record.get("V4"),
          record.get("V5"),
          record.get("V6"),
          record.get("V7"),
          record.get("V8"),
          record.get("V9"),
          record.get("V10"),
          record.get("V11"),
          record.get("V12"),
          record.get("V13"),
          record.get("V14"),
          record.get("V15"),
          record.get("V16"),
          record.get("V17"),
          record.get("V18"),
          record.get("V19"),
          record.get("V20"),
          record.get("V21"),
          record.get("V22"),
          record.get("V23"),
          record.get("V24"),
          record.get("V25"),
          record.get("V26"),
          record.get("V27"),
          record.get("V28"),
          record.get("Amount"),
          record.get("Class"))

      }, strType)
      
    //data stream from kafka
    val ds = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topics)
      .option("startingOffsets", "earliest")
//      .option("kafkaConsumer.pollTimeoutMs", batchDuration)//Default: 512
      .option("failOnDataLoss", "false")//For tests
      .load();
    
    
    

    sparkSession.udf.register("deserialize", u)

    val dataframe = ds.select("value").as(Encoders.BINARY).selectExpr("deserialize(value) AS rows").select("rows.*")

    var incomingTime: Long = System.currentTimeMillis

/********************************************************************
            * Apply Trained Random Forest
             ********************************************************************/

    val ds2 = assembler.transform(dataframe)

    val predictions = model.transform(ds2);
    val predictionsWithTime = predictions.withColumn("incoming_time", lit(incomingTime)).withColumn("prediction_time", current_timestamp())


    val writer = predictionsWithTime.select("uuid", "id", "prediction", "predictedLabel", "label", "incoming_time", "prediction_time")
      .writeStream
      .format("csv")
      .option("checkpointLocation", outputDirectory + "checkpoint")
      
      .option("path", outputDirectory + "predictions.csv")
      .trigger(Trigger.ProcessingTime("0 seconds"))//Start each micro batch as fast as it can with no delays 
//      .trigger(Trigger.Continuous("1 second"))//Continuous processing with checkpoint every 1 second
      .start();
    
    
    writer.awaitTermination();

  }
  
  
    def udfWithSeconds(bytes: Array[Byte]) = {
      val parser = new Schema.Parser();
      val schema = parser.parse(schemaJsonWithSecond);
      val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
      val decoder: Decoder = DecoderFactory.get().binaryDecoder(bytes, null)
      val record: GenericRecord = reader.read(null, decoder)

      Row(
        record.get("uuid").toString(),
        record.get("id"),
        record.get("NumericSecondDay"),
        record.get("V1"),
        record.get("V2"),
        record.get("V3"),
        record.get("V4"),
        record.get("V5"),
        record.get("V6"),
        record.get("V7"),
        record.get("V8"),
        record.get("V9"),
        record.get("V10"),
        record.get("V11"),
        record.get("V12"),
        record.get("V13"),
        record.get("V14"),
        record.get("V15"),
        record.get("V16"),
        record.get("V17"),
        record.get("V18"),
        record.get("V19"),
        record.get("V20"),
        record.get("V21"),
        record.get("V22"),
        record.get("V23"),
        record.get("V24"),
        record.get("V25"),
        record.get("V26"),
        record.get("V27"),
        record.get("V28"),
        record.get("Amount"),
        record.get("Class"))

    }
    
    def udfWithoutSeconds(bytes: Array[Byte]) = {
        val parser = new Schema.Parser();
        val schema = parser.parse(schemaJson);
        val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
        val decoder: Decoder = DecoderFactory.get().binaryDecoder(bytes, null)
        val record: GenericRecord = reader.read(null, decoder)

        Row(
          record.get("uuid").toString(),
          record.get("id"),
          record.get("V1"),
          record.get("V2"),
          record.get("V3"),
          record.get("V4"),
          record.get("V5"),
          record.get("V6"),
          record.get("V7"),
          record.get("V8"),
          record.get("V9"),
          record.get("V10"),
          record.get("V11"),
          record.get("V12"),
          record.get("V13"),
          record.get("V14"),
          record.get("V15"),
          record.get("V16"),
          record.get("V17"),
          record.get("V18"),
          record.get("V19"),
          record.get("V20"),
          record.get("V21"),
          record.get("V22"),
          record.get("V23"),
          record.get("V24"),
          record.get("V25"),
          record.get("V26"),
          record.get("V27"),
          record.get("V28"),
          record.get("Amount"),
          record.get("Class"))

      }

  final def schemaJson = """{
            | "namespace": "uk.ac.manchester.rtccfd",
            | "type": "record",
            | "name": "transaction",
            | "fields": [ {"name": "uuid", "type": "string"},
            | {"name": "id", "type": "long"},
            | {"name": "V1", "type": "double"}, 
            | {"name": "V2", "type": "double"}, 
            | {"name": "V3", "type": "double"}, 
            | {"name": "V4", "type": "double"}, 
            | {"name": "V5", "type": "double"}, 
            | {"name": "V6", "type": "double"}, 
            | {"name": "V7", "type": "double"}, 
            | {"name": "V8", "type": "double"}, 
            | {"name": "V9", "type": "double"}, 
            | {"name": "V10", "type": "double"}, 
            | {"name": "V11", "type": "double"}, 
            | {"name": "V12", "type": "double"}, 
            | {"name": "V13", "type": "double"}, 
            | {"name": "V14", "type": "double"}, 
            | {"name": "V15", "type": "double"}, 
            | {"name": "V16", "type": "double"}, 
            | {"name": "V17", "type": "double"}, 
            | {"name": "V18", "type": "double"}, 
            | {"name": "V19", "type": "double"}, 
            | {"name": "V20", "type": "double"}, 
            | {"name": "V21", "type": "double"}, 
            | {"name": "V22", "type": "double"}, 
            | {"name": "V23", "type": "double"}, 
            | {"name": "V24", "type": "double"}, 
            | {"name": "V25", "type": "double"}, 
            | {"name": "V26", "type": "double"}, 
            | {"name": "V27", "type": "double"}, 
            | {"name": "V28", "type": "double"}, 
            | {"name": "Amount", "type": "double"}, 
            | {"name": "Class", "type": "int"}  ]
 
            | }""".stripMargin

  final def schemaJsonWithSecond = """{
            | "namespace": "uk.ac.manchester.rtccfd",
            | "type": "record",
            | "name": "transaction",
            | "fields": [{"name": "uuid", "type": "string"},
            | {"name": "id", "type": "long"},
            | {"name": "NumericSecondDay", "type": "int"},
            | {"name": "V1", "type": "double"}, 
            | {"name": "V2", "type": "double"}, 
            | {"name": "V3", "type": "double"}, 
            | {"name": "V4", "type": "double"}, 
            | {"name": "V5", "type": "double"}, 
            | {"name": "V6", "type": "double"}, 
            | {"name": "V7", "type": "double"}, 
            | {"name": "V8", "type": "double"}, 
            | {"name": "V9", "type": "double"}, 
            | {"name": "V10", "type": "double"}, 
            | {"name": "V11", "type": "double"}, 
            | {"name": "V12", "type": "double"}, 
            | {"name": "V13", "type": "double"}, 
            | {"name": "V14", "type": "double"}, 
            | {"name": "V15", "type": "double"}, 
            | {"name": "V16", "type": "double"}, 
            | {"name": "V17", "type": "double"}, 
            | {"name": "V18", "type": "double"}, 
            | {"name": "V19", "type": "double"}, 
            | {"name": "V20", "type": "double"}, 
            | {"name": "V21", "type": "double"}, 
            | {"name": "V22", "type": "double"}, 
            | {"name": "V23", "type": "double"}, 
            | {"name": "V24", "type": "double"}, 
            | {"name": "V25", "type": "double"}, 
            | {"name": "V26", "type": "double"}, 
            | {"name": "V27", "type": "double"}, 
            | {"name": "V28", "type": "double"}, 
            | {"name": "Amount", "type": "double"}, 
            | {"name": "Class", "type": "int"}  ]
 
            | }""".stripMargin
            
}
