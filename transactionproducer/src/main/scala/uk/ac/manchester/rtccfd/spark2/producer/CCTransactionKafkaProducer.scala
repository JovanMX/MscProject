package uk.ac.manchester.rtccfd.spark2.producer

import java.util.{ Properties, UUID }
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.io._
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.slf4j.LoggerFactory
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Formatter.DateTime
import java.util.Formatter.DateTime
import org.apache.kafka.common.utils.Bytes
import java.io.PrintWriter
import java.io.FileWriter
import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.RecordMetadata
import java.util.concurrent.locks.LockSupport
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.specific.SpecificDatumWriter
import java.io.ByteArrayOutputStream

class CCTransactionKafkaProducer(bootstrapServers: String, topic: String) {
  val logger = LoggerFactory.getLogger(getClass)

  private val props = new Properties()
  props.put("bootstrap.servers", bootstrapServers)
  props.put("message.send.max.retries", "5")

  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
//  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[BytesSerializer])
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
  
  val producer = new KafkaProducer[String, Array[Byte]](props)


  def start(dir: String, transactions: Seq[GenericRecord], numProducers: Int, tps: Double, numCycles: Int, withSeconds : Boolean) {

    logger.info("start()")

    /*
   * Load test dataset
   */
    try {  
      val intervalTime = Math.floor(1000.0 / tps).toLong
      var starttime : Long = 0
      var countThroughput = 0
      var nextPrint : Long = 0

      val pw = new PrintWriter(new FileWriter(dir + "log/producerLog-"+UUID.randomUUID().toString()+".txt"))
      
      Runtime.getRuntime().addShutdownHook(new Thread() {
        override def run() {
          println("Shutdown Hook. Closing printer...");
          pw.close();
        }
      });

      try {
        
        for (i <- 0 until numCycles) {
        
        
          starttime = System.currentTimeMillis()
          nextPrint = starttime + 5000
          transactions.foreach {
            avroRecord =>
  
              //      val record = new ProducerRecord<>("mytopic", bytes);
              
              
              val uuid = UUID.randomUUID().toString()
              avroRecord.put("uuid", uuid)
              val id = avroRecord.get("id")
              
              val writer = new SpecificDatumWriter[GenericRecord](if(withSeconds) CCTransactionCSVReader.schemaWithSeconds else CCTransactionCSVReader.schema)
              val out = new ByteArrayOutputStream()
              val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(out, null)
              writer.write(avroRecord, encoder)
              encoder.flush()
              out.close()
              val serializedBytes: Array[Byte] = out.toByteArray()
      
      
              val record = new ProducerRecord[String, Array[Byte]](topic, uuid, serializedBytes)
              
  				    LockSupport.parkNanos(Math.max(0, starttime - System.currentTimeMillis()) * 1000000)
  				    val time = System.currentTimeMillis() //Temporal
              producer.send(
                record,
  
                new Callback() {
                  override def onCompletion(metadata: RecordMetadata, e: Exception) {
                    if (e != null)
                      e.printStackTrace();
                  }
                })
                
                pw.println(new StringBuilder(uuid).append(",").append(id).append(",").append(time).toString());
                pw.flush();
  
              
              countThroughput = countThroughput + 1
              if (nextPrint <= System.currentTimeMillis()) {
                nextPrint = nextPrint + 5000
                logger.info("Number of transactions per second:" + (countThroughput / 5))
                countThroughput = 0
              }
  				starttime = starttime + intervalTime
  
          }
        }
      } catch {
        case e: Throwable => e.printStackTrace();
      } finally {
        System.out.println("Closing  printer...");
        pw.close();
      }
    } catch {
      case e: Throwable => logger.error("Error in Producer.", e); e.printStackTrace()
    }

  }

  class KafkaTransactionCallback extends Callback {

    import java.util.concurrent.atomic.AtomicReference

    private val lastException = new AtomicReference[Option[Exception]](None)

    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
      Option(exception).foreach { ex => lastException.set(Some(ex)) }

    def throwExceptionIfAny(): Unit = lastException.getAndSet(None).foreach(ex => throw ex)

  }

}
