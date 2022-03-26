package streaming.flume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils

object flumeshujuyuan {
  def main(args: Array[String]): Unit = {

    //实际生产使用
    if(args.length!=2){
      System.err.println("192.168.43.133,192.168.43.134,192.168.43.135",44444)
      System.exit(1)
    }
    val Array(hostname,port)=args

    var sparkConf=new SparkConf() //.setMaster("local[2]").setAppName("FlumePullWordCount_product")
    val ssc=new StreamingContext(sparkConf,Seconds(5))

    //TODO:如何使用Sparkfluming 整合flume
    val flumeStream= FlumeUtils.createPollingStream(ssc,hostname,port.toInt)

    flumeStream.map(x=>new String(x.event.getBody.array()).trim)
      .flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()

    ssc.start()
    ssc.awaitTermination()

  }
}
