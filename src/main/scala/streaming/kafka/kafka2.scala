package streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
object kafka2 {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("local[*]").setAppName("asm")
    var sc = new StreamingContext(conf, Seconds(5))
    val kafkaparams = Map(
      "metadata.broker.list" -> "192.168.43.133:9092,192.168.43.134:9092,192.168.43.135:9092",
      "group.id" -> "kafka_Direct"
    )
    val topics = Set("kafkatopic", "fiest")
    val streams: InputDStream[(String, String)]  = KafkaUtils.createDirectStream[String,String, StringDecoder, StringDecoder](sc,kafkaparams,topics)
    /*
            只用kafkaUtil 当做数据源读取数据（接收器模式）
        */
    var zkServer = "192.168.43.133:9092,192.168.43.134:9092,192.168.43.135:9092"

    val topics1 = Map[String, Int]("kafkatopic" -> 1)

    val line1: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(sc, zkServer, "p1", topics1)

    val line: DStream[String] = line1.map(_._2)

    val reduced: DStream[(String, Int)] = line.flatMap(_.split(" ").map((_, 1))).reduceByKey(_ + _)
    reduced.print()

  }
}
