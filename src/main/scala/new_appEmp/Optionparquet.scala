//package new_appEmp
//
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//import 测试样例类.样例类.SubmitTask
//
//import scala.util.Random
//
//object Optionparquet extends App{
//  override def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    var spark = SparkSession
//      .builder()
//      .config(conf)
//      .appName("OptionDemo")
//      .master("local[*]")
//      .getOrCreate()
//    val sc = spark.sparkContext
//    var Array(inputPath,inputoutput)=args
//    val txt = sc.textFile(inputPath)
//    val txtArray = Array(txt)
////    val unit = txtArray(Random.nextInt(txtArray.length)) match {
//////      case SubmitTask(id, name) => {
//////        println(s"$id,$name")
//////      }
////
//////    }
////  }
//}
