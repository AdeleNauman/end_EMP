package streaming

import streaming.自定义数据源.CustomerReceiver

object 自定义读取文件 {
  def main(args: Array[String]): Unit = {

    val receiver = new CustomerReceiver("aaa",2)
    println(receiver.receive())


  }
}
