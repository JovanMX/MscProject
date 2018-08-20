package uk.ac.manchester.rtccfd.spark2;



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

object StreamingFraudDetection {

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
    val modelDirerctory= CommonUtils.prepareDirectoryPath(args(2))
    val outputDirectory = CommonUtils.prepareDirectoryPath(args(3))
    val batchDurationQuantity = args(4).toInt
    val batchDurationUnit = args(5)
    val withSeconds = if (args.length < 7 || args(6)!="1") false else true
    
    val batchDuration = if (batchDurationUnit == "ms") Milliseconds(batchDurationQuantity) else Seconds(batchDurationQuantity)


    val sparkSession = SparkSession.builder().getOrCreate();
    //  val sc = SparkContext.getOrCreate(conf);
    val ssc = new StreamingContext(sparkSession.sparkContext, batchDuration);

    // Load the Trained Model
    val model = CrossValidatorModel.read.load(modelDirerctory + "randomForestClassifier");

/********************************************************************
     * KAFKA CONSUMER DSTREAM
     ********************************************************************/

    // Specify the Kafka Broker Options and set of Topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[ByteArrayDeserializer],
      "group.id" -> "defaut",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean) //commit will be done programmatically
    )
    val topicSet = topics.split(",").toSet;

    log.info("brokers:" + brokers)
    log.info("topicSet:" + topicSet)

    // Create an input DStream using KafkaUtils
    log.info("KafkaUtils.createDirectStream() -before")
    val messages = KafkaUtils.createDirectStream[String, Array[Byte]](
      ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, Array[Byte]](topicSet, kafkaParams));
    log.info("KafkaUtils.createDirectStream() -after")
/********************************************************************
     * DESERIALISE USING INVERSION, PRE-PROCESS AND CLASSIFY
     ********************************************************************/

    var offsetRanges = Array[OffsetRange]()
    var incomingTime: Long = 0
    val parserRow = if(withSeconds) parseRowWithSecond(_) else parseRow(_) 

    messages.
      transform { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        incomingTime = System.currentTimeMillis
        rdd
      }
      .map(parserRow(_))
      .foreachRDD(rdd => {

        // Check that the RDD is not null
        if (rdd != null && !rdd.isEmpty()) {

          // Convert the RDD of Rows to a DataFrame
          val dataframe = sparkSession.createDataFrame(
            rdd, if(withSeconds)dfSquemaWithSecond else dfSquema);

/********************************************************************
            * Apply Trained Random Forest
             ********************************************************************/

          val assembler = if(withSeconds) new VectorAssembler().setInputCols(Array("NumericSecondDay", "V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10", "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18", "V19", "V20", "V21", "V22", "V23", "V24", "V25", "V26", "V27", "V28", "Amount")).setOutputCol("features")
            else new VectorAssembler().setInputCols(Array("V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10", "V11", "V12", "V13", "V14", "V15", "V16", "V17", "V18", "V19", "V20", "V21", "V22", "V23", "V24", "V25", "V26", "V27", "V28", "Amount")).setOutputCol("features")
          
          val ds2 = assembler.transform(dataframe)

          val predictions = model.transform(ds2);
          val predictionsWithTime = predictions.withColumn("incoming_time", lit(incomingTime)).withColumn("prediction_time", current_timestamp())

          predictionsWithTime.select("uuid", "id", "prediction", "predictedLabel", "label", "incoming_time", "prediction_time").write.mode(SaveMode.Append).csv(outputDirectory + "predictions.csv")

        }
        //after results are saved
        messages.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      })
    // Start the computation
    ssc.start();
    // Wait for the computation to terminate
    ssc.awaitTermination();
  }

    // Deserialise the Avro messages using Bijection and parse to Row
  def parseRow(message: ConsumerRecord[String, Array[Byte]]): Row = {

    val parser = new Schema.Parser();
    val schema = parser.parse(schemaJson);
    //creating Injection object for avro
//    val recordInjection = GenericAvroCodecs.toBinary[GenericRecord](schema);
//    val tryRecord = recordInjection.invert(message.value());
//    val record = tryRecord.get
    
        // Deserialize and create generic record
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(message.value(), null)
    val record: GenericRecord = reader.read(null, decoder)

    Row(
      message.key(),
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
def parseRowWithSecond(message: ConsumerRecord[String, Array[Byte]]): Row = {

    val parser = new Schema.Parser();
    val schema = parser.parse(schemaJsonWithSecond);
    //creating Injection object for avro
//    val recordInjection = GenericAvroCodecs.toBinary[GenericRecord](schema);
//    val tryRecord = recordInjection.invert(message.value());
//    val record = tryRecord.get
    
        // Deserialize and create generic record
    val reader: DatumReader[GenericRecord] = new SpecificDatumReader[GenericRecord](schema)
    val decoder: Decoder = DecoderFactory.get().binaryDecoder(message.value(), null)
    val record: GenericRecord = reader.read(null, decoder)

    Row(
      message.key(),
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

  final def schemaJson = """{
            | "namespace": "uk.ac.manchester.rtccfd",
            | "type": "record",
            | "name": "transaction",
            | "fields": [{"name": "uuid", "type": "string"},
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

  val dfSquema = new StructType(Array(
    StructField("uuid", StringType, true),
    StructField("id", LongType, true),
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
    StructField("Class", IntegerType, false)))
  
  val dfSquemaWithSecond = new StructType(Array(
    StructField("uuid", StringType, true),
    StructField("id", LongType, true),
    StructField("NumericSecondDay", IntegerType, true),
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
    StructField("Class", IntegerType, false)))
}
