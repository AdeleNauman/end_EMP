package streaming.基础代码

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object nc链接 {
  def main(args: Array[String]): Unit = {

    val streaming = new SparkConf().setMaster("local[*]").setAppName("streaming")
    val sc = new StreamingContext(streaming, Seconds(1))

    val lines = sc.socketTextStream("192.168.43.133", 9999)
    val flatt = lines.flatMap(_.split(" "))
    val mapp = flatt.map(lien => (lien, 1))
    val wordCounts = mapp.reduceByKey(_ + _)
    wordCounts.print()
    sc.start()
    sc.awaitTermination()
  }
}
