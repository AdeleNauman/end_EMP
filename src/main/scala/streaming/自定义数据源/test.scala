package streaming.自定义数据源

object test {
  def main(args: Array[String]): Unit = {
    val receiver = new CustomerReceiver("192.168.43.133",9999)
    receiver.onStart()
  }
}
