package uk.ac.manchester.rtccfd.spark2.producer

import org.apache.log4j.BasicConfigurator
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

object Main extends App{
  
    BasicConfigurator.configure()
    val logger = LoggerFactory.getLogger(getClass)
    logger.info("Starting the application")
    
    try{
      
      
    
    logger.info("args.length = "+args.length)
    
    if ( args.length < 1 ) {
            System.err.println("Usage: Main <throughtput Per Second>");
            System.exit(1);
        }

    val bootstrapServers = args(0)
    val topic = args(1)
    val testDataDir = args(2)
    val numProducers = args(3).toInt
    val throughtputPerSecond = args(4).toDouble
    val minutesExecution = args(5).toInt
    val withSeconds = if (args.length < 7 || args(6)!="1") false else true
    val numCycles = if(args.length<8) 1 else args(7).toInt
    
    
    logger.info("bootstrapServers = "+bootstrapServers)
    logger.info("testDataDir = "+testDataDir)
    logger.info("throughtputPerSecond = "+throughtputPerSecond)
    logger.info("minutesExecution = "+minutesExecution)
    logger.info("withSeconds = "+withSeconds)
    
    val transactions = CCTransactionCSVReader.readCCTransaction(testDataDir + "creditcard_test_partition.csv", withSeconds)
      
      val service = Executors.newCachedThreadPool();
      final class MyTask extends Runnable {
  
          def run() {
              val producer = new CCTransactionKafkaProducer(bootstrapServers, topic)
              producer.start(testDataDir, transactions, numProducers, throughtputPerSecond, numCycles, withSeconds)
          }
      }
      for (i <- 0 until numProducers) {
          service.submit(new MyTask());
      }
      service.shutdown();
      service.awaitTermination(minutesExecution, TimeUnit.MINUTES);
      
    }
    catch {
      case e: Throwable => e.printStackTrace()
    }

  
}