package streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object kafka1 {
  def main(args: Array[String]): Unit = {
    val rdd = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new StreamingContext(rdd, Seconds(5))
    val kafkaParams = Map("metadata.broker.list" -> "192.168.137.66:9092,192.168.137.67:9092,192.168.137.68:9092",
      "group.id" -> "kafka_Driect")

    val topis = Set("first")

    val dstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](sc, kafkaParams, topis)
    val topicData = dstream.map(_._2)
    val wordAndOne = topicData.flatMap(_.split(" ")).map((_, 1))

    val resultDS = wordAndOne.reduceByKey(_ + _)
    resultDS.print()
    sc.start()
    sc.awaitTermination()
  }
}
