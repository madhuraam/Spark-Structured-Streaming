
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer,ProducerRecord}

import scala.annotation.tailrec
import scala.util.{Random => r}

object kafkaProducer {



  def main(args :Array[String]) :Unit = {

    val props = new Properties()
    props.put("bootstrap.servers","ip-172-31-20-247.ec2.internal:6667")
    props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val interval = 1000
    val topic = "cars"
    val numRecsToProduce : Option[Int] = None

    @tailrec
    def produceRecord(numRecToProduce : Option[Int]) :Unit = {
      def generateCarRecord(topic: String) : ProducerRecord[String,String] = {
        val carName = s"car${r.nextInt(10)}"
        val speed = r.nextInt(150)
        val acc = r.nextFloat * 100

        val value = s"$carName,$speed,$acc,${System.currentTimeMillis()}"
        print(s"Writing $value\n")
        val d = r.nextFloat() * 100

        if(d < 2 ) {
          println("Argh! some network delay")
          Thread.sleep((d*100).toLong)

        }
        new ProducerRecord[String,String](topic,"key", value)

      }

      numRecsToProduce match {
        case Some(x) if x > 0 =>
          producer.send(generateCarRecord(topic))
          Thread.sleep(interval)
          produceRecord(Some(x - 1))

        case None =>
          producer.send(generateCarRecord(topic))
          Thread.sleep(interval)
          produceRecord(None)

        case _ =>

      }
    }
    produceRecord(numRecsToProduce)
  }

}
