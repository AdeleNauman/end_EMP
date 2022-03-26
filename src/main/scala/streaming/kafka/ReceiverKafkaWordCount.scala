package streaming.kafka


import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReceiverKafkaWordCount {
  Logger.getLogger("org").setLevel(Level.ERROR)
//  def main(args: Array[String]): Unit = {
//    val array:(brokers, topics) = Array(Conf.KAFKA_BROKER, Conf.TEST_TOPIC)
//
//    // Create context with 2 second batch interval
//    val conf = new SparkConf()
//      .setMaster("local")
//      .setAppName("OnlineStreamHobby") //设置本程序名称
//    //      .set("auto.offset.reset","smallest")
//    val ssc = new StreamingContext(conf, Seconds(2))
//    //    从kafka取数据
//    val kafkaParams: Map[String, String] = Map[String, String](
//      //      "auto.offset.reset" -> "smallest", //自动将偏移重置为最早的偏移
//      "zookeeper.connect" -> Conf.ZK_HOST,
//      //      "bootstrap.servers" -> Common.KAFKA_BROKER_LIST,
//      "group.id" -> "test"
//    )
//    val numThreads = 1
//    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
//    val fact_streaming = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_2).map(_._2)
//    //    fact_streaming.print()
//    val words = fact_streaming.flatMap(_.split(" "))
//    val wordCounts = words.map(x => (x, 1L)).reduceByKey(_ + _)
//    wordCounts.print()
//    ssc.checkpoint(".")
//    //启动spark并设置执行时间
//    ssc.start()
//    ssc.awaitTermination()
//  }
}