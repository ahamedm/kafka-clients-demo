package dev.ahamedm.demo.prod

import java.util.Properties
import java.util.concurrent.ThreadLocalRandom

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.joda.time.DateTime
import scopt.OParser

case class FlightEvent(fltNo:String, origin:String,destination:String,fltDate:DateTime, eventDesc:String, evtDate:DateTime){
   override def toString = s"$fltNo#$origin#$destination#$fltDate#$eventDesc"
}

case class AppConf(topic :String ="tickets_sample_t1",brokers:String="192.168.56.101:9092",schedule:Int=2000)

object Main {
  val builder = OParser.builder[AppConf]
  val parser = new scopt.OptionParser[AppConf]("Demo-Producer") {
      head("Demo Producer", "0.x")

      opt[String]('t', "topic")
        .action((x, c) => c.copy(topic = x))
        .text("Topic to publish message")

      opt[String]('b', "brokers")
        .action((x, c) => c.copy(brokers = x))
        .text("Brokers URL host:port")

      opt[Int]('s', "schedule")
        .action((x, c) => c.copy(schedule = x))
        .text("Schedule of event gen")

     help("help").text("prints this usage text")


  }

  val origins:Array[String] = Array("DXB", "BMB", "MAA", "CMB","SIN","JFK","LHR", "LCY","LGW")
  val destinations:Array[String] = Array("DXB", "BMB", "MAA", "CMB","SIN","JFK","LHR", "LCY","LGW", "KSA")
  val randomFltNo:()=>String = ()=> {
      "EK"+ThreadLocalRandom.current().nextInt(200,300)
  }
  var fltEvents:Array[String] = Array("Arrived", "Departed","Airborne","Cancelled","Chock-On", "Chock-Off")

  val genFltEvent:()=>FlightEvent=()=>{
      val od_idx=ThreadLocalRandom.current().nextInt(0,8)
      val evt_idx=ThreadLocalRandom.current().nextInt(0,5)
      FlightEvent(randomFltNo(),origins(od_idx),destinations(od_idx+1),new DateTime(),fltEvents(evt_idx),new DateTime())

  }

  def loop(topic:String,producer:KafkaProducer[String,String],schedule:Int): Nothing = {
    val randomFltEvent:FlightEvent = genFltEvent()
    producer.send(new ProducerRecord[String,String](topic,randomFltEvent.fltNo,randomFltEvent.toString))
    Thread.sleep(schedule)
    loop(topic,producer,schedule)
  }

  def main(args:Array[String]):Unit = {

    parser.parse(args, AppConf()) match {
      case Some(config) =>
        println(config)
        val props = new Properties()
        props.put("bootstrap.servers", config.brokers)
        props.put("client.id", "FltEventProducer")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val producer:KafkaProducer[String,String] = new KafkaProducer[String,String](props )

        loop(config.topic,producer,config.schedule)

      case _ =>
        // arguments are bad, error message will have been displayed
    }


  }
}
