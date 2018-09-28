
import java.sql.Timestamp

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object KafkaSourceStreaming {

  //convert aggregates into typed data
  case class CarEvent(carId: String, speed: Option[Int], acceleration: Option[Double], timestamp: Timestamp)
  object CarEvent {
    def apply(rawStr: String): CarEvent = {
      val parts = rawStr.split(",")
      CarEvent(parts(0), Some(Integer.parseInt(parts(1))), Some(java.lang.Double.parseDouble(parts(2))), new Timestamp(parts(3).toLong))
    }
  }

  def main(args: Array[String]): Unit = {

    //create a spark session, and run it on local mode
    val spark = SparkSession.builder()
      .appName("KafkaSourceStreaming")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    //read the source
    val df: DataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-20-247.ec2.internal:6667")
      .option("subscribe", "cars")
      //.schema(schema)  : we cannot set a schema for kafka source. Kafka source has a fixed schema of (key, value)
      .load()

    val cars: Dataset[CarEvent] = df
      .selectExpr("CAST(value AS STRING)")
      .map(r => CarEvent(r.getString(0)))

    //aggregation without window
    /*val aggregates = cars
      .groupBy("carId")
      .avg("speed")*/

    //windowing
    val aggregates = cars
      .withWatermark("timestamp", "3 seconds")
      //.groupBy(window($"timestamp","4 seconds","1 seconds"), $"carId")  //sliding window of size 4 seconds, that slides every 1 second
      .groupBy(window($"timestamp","4 seconds"), $"carId") //tumbling window of size 4 seconds (event time)
      //.groupBy(window(current_timestamp(),"4 seconds"), $"carId") //Use processing time.
      .agg(avg("speed").alias("speed"))
    //.where("speed > 70")

    aggregates.printSchema()



    val writeToConsole = aggregates
      .writeStream
      .format("console")
      .option("truncate", "false") //prevent trimming output fields
      .queryName("kafka spark streaming console")
      .outputMode("update")
      .start()

  val writeToKafka = aggregates
      .selectExpr("CAST(carId AS STRING) AS key", "CAST(speed AS STRING) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers","ip-172-31-20-247.ec2.internal:6667")
      .option("topic", "fastcars")
      //.option("startingOffsets", "earliest") //earliest, latest or offset location. default latest for streaming
      //.option("endingOffsets", "latest") // used only for batch queries
      .option("checkpointLocation", "hdfs://ip-172-31-35-141.ec2.internal:8020//user/jjraam1018/Data") //must when not memory or console output
      .queryName("kafka spark streaming kafka")
      //.outputMode("complete") // output everything
      //.outputMode("append")  // only supported when we set watermark. output only new
      .outputMode("update") //ouput new and updated
      .start()

    spark.streams.awaitAnyTermination() //running multiple streams at a time
  }

}
