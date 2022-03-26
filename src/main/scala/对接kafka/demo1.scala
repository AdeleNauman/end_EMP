package 对接kafka

import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object demo1 {
  def main(args: Array[String]): Unit = {

    val conff = new SparkConf().setMaster("local[*]").setAppName("rdd")
    val sc = new StreamingContext(conff, Seconds(3))
//    val value = KafkaUtils.createDirectStream[String, String]()

  }

}
