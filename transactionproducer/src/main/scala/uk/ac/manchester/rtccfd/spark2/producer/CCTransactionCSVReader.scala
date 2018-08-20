package uk.ac.manchester.rtccfd.spark2.producer
import scala.io.Source
import org.apache.avro.generic.GenericData
import org.apache.avro.Schema
import org.slf4j.LoggerFactory
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord
//import com.twitter.bijection.avro.GenericAvroCodecs
//import com.twitter.bijection.Injection
import java.util.UUID
import org.apache.avro.generic.GenericData.Record
import org.apache.kafka.common.utils.Bytes
import org.apache.avro.specific.SpecificDatumWriter
import java.io.ByteArrayOutputStream
import org.apache.avro.io.EncoderFactory
import org.apache.avro.io.BinaryEncoder

object CCTransactionCSVReader {
  

val logger = LoggerFactory.getLogger(getClass)

val schema  = new Schema.Parser().parse("""{
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
 
            | }""".stripMargin)
            
            
val schemaWithSeconds  = new Schema.Parser().parse("""{
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
 
            | }""".stripMargin)
             
            
//val recordInjection : Injection[GenericRecord, Array[Byte]]  = GenericAvroCodecs.toBinary(schema)


def parseDouble(s: String) = try { s.toDouble } catch { case _ : Throwable => None }
def parseLong(s: String) : Long = {try { s.toLong } catch { case _ : Throwable => None; 0}}
def parseInt(s: String) = try { s.toInt } catch { case _ : Throwable => None }


  def readCCTransaction(fileName: String, withSeconds: Boolean): Seq[GenericRecord] = {


    for {
      line <- Source.fromFile(fileName).getLines().drop(1).toVector
      values = line.split(",").map(_.trim)
    } yield {

      val avroRecord = new GenericData.Record(if(withSeconds) schemaWithSeconds else schema)
      val id = parseLong(values(0))
      avroRecord.put("id", id)
      
      val adjustFeatureNumber = if(withSeconds) 0 else -1
      if(withSeconds)
        avroRecord.put("NumericSecondDay", parseInt(values(1+adjustFeatureNumber)))
        
      avroRecord.put("V1", parseDouble(values(2+adjustFeatureNumber)))
      avroRecord.put("V2", parseDouble(values(3+adjustFeatureNumber)))
      avroRecord.put("V3", parseDouble(values(4+adjustFeatureNumber)))
      avroRecord.put("V4", parseDouble(values(5+adjustFeatureNumber)))
      avroRecord.put("V5", parseDouble(values(6+adjustFeatureNumber)))
      avroRecord.put("V6", parseDouble(values(7+adjustFeatureNumber)))
      avroRecord.put("V7", parseDouble(values(8+adjustFeatureNumber)))
      avroRecord.put("V8", parseDouble(values(9+adjustFeatureNumber)))
      avroRecord.put("V9", parseDouble(values(10+adjustFeatureNumber)))
      avroRecord.put("V10", parseDouble(values(11+adjustFeatureNumber)))
      avroRecord.put("V11", parseDouble(values(12+adjustFeatureNumber)))
      avroRecord.put("V12", parseDouble(values(13+adjustFeatureNumber)))
      avroRecord.put("V13", parseDouble(values(14+adjustFeatureNumber)))
      avroRecord.put("V14", parseDouble(values(15+adjustFeatureNumber)))
      avroRecord.put("V15", parseDouble(values(16+adjustFeatureNumber)))
      avroRecord.put("V16", parseDouble(values(17+adjustFeatureNumber)))
      avroRecord.put("V17", parseDouble(values(18+adjustFeatureNumber)))
      avroRecord.put("V18", parseDouble(values(19+adjustFeatureNumber)))
      avroRecord.put("V19", parseDouble(values(20+adjustFeatureNumber)))
      avroRecord.put("V20", parseDouble(values(21+adjustFeatureNumber)))
      avroRecord.put("V21", parseDouble(values(22+adjustFeatureNumber)))
      avroRecord.put("V22", parseDouble(values(23+adjustFeatureNumber)))
      avroRecord.put("V23", parseDouble(values(24+adjustFeatureNumber)))
      avroRecord.put("V24", parseDouble(values(25+adjustFeatureNumber)))
      avroRecord.put("V25", parseDouble(values(26+adjustFeatureNumber)))
      avroRecord.put("V26", parseDouble(values(27+adjustFeatureNumber)))
      avroRecord.put("V27", parseDouble(values(28+adjustFeatureNumber)))
      avroRecord.put("V28", parseDouble(values(29+adjustFeatureNumber)))
      avroRecord.put("Amount", parseDouble(values(30+adjustFeatureNumber)))
      avroRecord.put("Class", parseInt(values(31+adjustFeatureNumber)))
      
//      avroRecord.put("V1", parseDouble(values(1)))
//      avroRecord.put("V2", parseDouble(values(2)))
//      avroRecord.put("V3", parseDouble(values(3)))
//      avroRecord.put("V4", parseDouble(values(4)))
//      avroRecord.put("V5", parseDouble(values(5)))
//      avroRecord.put("V6", parseDouble(values(6)))
//      avroRecord.put("V7", parseDouble(values(7)))
//      avroRecord.put("V8", parseDouble(values(8)))
//      avroRecord.put("V9", parseDouble(values(9)))
//      avroRecord.put("V10", parseDouble(values(10)))
//      avroRecord.put("V11", parseDouble(values(11)))
//      avroRecord.put("V12", parseDouble(values(12)))
//      avroRecord.put("V13", parseDouble(values(13)))
//      avroRecord.put("V14", parseDouble(values(14)))
//      avroRecord.put("V15", parseDouble(values(15)))
//      avroRecord.put("V16", parseDouble(values(16)))
//      avroRecord.put("V17", parseDouble(values(17)))
//      avroRecord.put("V18", parseDouble(values(18)))
//      avroRecord.put("V19", parseDouble(values(19)))
//      avroRecord.put("V20", parseDouble(values(20)))
//      avroRecord.put("V21", parseDouble(values(21)))
//      avroRecord.put("V22", parseDouble(values(22)))
//      avroRecord.put("V23", parseDouble(values(23)))
//      avroRecord.put("V24", parseDouble(values(24)))
//      avroRecord.put("V25", parseDouble(values(25)))
//      avroRecord.put("V26", parseDouble(values(26)))
//      avroRecord.put("V27", parseDouble(values(27)))
//      avroRecord.put("V28", parseDouble(values(28)))
//      avroRecord.put("Amount", parseDouble(values(29)))
//      avroRecord.put("Class", parseInt(values(30)))
      avroRecord
    }
  }
}