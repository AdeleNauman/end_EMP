package 自定义数据源

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object 自定义1 {

  def main(args: Array[String]): Unit = {

    //配置streaming环境
    val streaming = new SparkConf().setMaster("local[*]").setAppName("streaming")
    //
    val context = new StreamingContext(streaming,Seconds(3))

    val value = context.receiverStream(new MyReceiver)
    value.print(  )
    //启动任务
    //初始化任务
    context.start()
    //抛出异常
    context.awaitTermination()
  }
  //自定义数据采集器 Receiver
  // 继承Receiver 并重写其方法
  //
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
    private var flag = true
    override def onStart(): Unit = {
      new Thread(new Runnable {

          override def run(): Unit ={
            while (flag) {
              val message ="采集数据为："+ new Random().nextInt(10).toString//随机生成字符串
              store(message)//封装数据 -- 这个封装级别就是MEMORY_ONLY
              Thread.sleep(500)
            }
          }
        }
      ).start()
    }

    override def onStop(): Unit = {
      flag = false
    }

  }

}
