
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka._
import org.joda.time.DateTime

object Main {

   def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("demo-app").setMaster("local[5]")
    val ssc = new StreamingContext(conf, Seconds(180))

    val kafkaConf = Map(
      "bootstrap.servers" -> "192.168.56.101:9092",
      "group.id" -> "demo-consumer-h",
      "key.deserializer" -> classOf[StringDeserializer].getName,
      "value.deserializer" -> classOf[StringDeserializer].getName,
      "zookeeper.connect" -> "192.168.56.101:2181",
      "zookeeper.connection.timeout.ms" -> "2000",
      "auto.offset.reset" -> "smallest",
      "offsets.storage" -> "kafka"
    )

    val fnStringToCase :(String=>FlightEvent) = (ip:String)=>{
      val tokens:Array[String]=ip.split("#")
      FlightEvent(tokens(0),tokens(1),tokens(2),new DateTime(tokens(3)),tokens(4),new DateTime())
    }
    /*val kafkaStream:ReceiverInputDStream[(String,String)] = KafkaUtils.createStream(ssc,kafkaConf,
                                      Map("tickets_sample_t1"->2),
                                      StorageLevels.MEMORY_ONLY)
    */
    val kafkaStream = KafkaUtils.createStream(ssc,"192.168.56.101:2181","demo-consumer-h",
            Map("tickets_sample_t1"->2),
            StorageLevels.MEMORY_ONLY)


    //val kafkaStream:ReceiverInputDStream[(String,String)] = KafkaUtils.createStream(ssc, kafkaConf,Map("tickets_Sample_t1"->2),StorageLevels.MEMORY_ONLY)


    /*
    var offsetRanges = Array.empty[OffsetRange]
    kafkaStream.transform{ rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
    }*/


    kafkaStream.map{
      rdd => rdd._2
    }.print()

    /*
    map{
      events => fnStringToCase(events)/* {
        val tokens:Array[String]=events.split("#")
        FlightEvent(tokens(0),tokens(1),tokens(2),new DateTime(tokens(3)),tokens(4),new DateTime())
      }*/
    }*/

    /*foreachRDD {
      fltEvtRDD =>
        fltEvtRDD.foreach{fe => println(s"Received Flt Event $fe.eventDesc for $fe.fltNo on $fe.evtDate ")}
    }*/

    ssc.start()
    ssc.awaitTermination()
  }

}

case class FlightEvent(fltNo:String, origin:String,destination:String,fltDate:DateTime, eventDesc:String, evtDate:DateTime)
