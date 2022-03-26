package new_appEmp.tools

import new_appEmp.bean.LogBean
import new_appEmp.utils.LogSchema
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Log2Parquet2 {
  def main(args: Array[String]): Unit = {
    if(args.length !=2){
      println(
        """
          |缺少参数
          |
          |""".stripMargin)
      sys.exit()
    }
    //引入环境
    var conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    //将自定义对象进行kryo序列化
    val spark = SparkSession
      .builder()
      .config(conf)
      .appName("rdd")
      .master("local[*]")
      .getOrCreate()
    var sc = spark.sparkContext
    //序列化转换后 调用隐士转换 使用dateformat存储数据
    import spark.implicits._
    //接受参数
    var Array(sparkinput,sparkoutput)=args
    val value = sc.textFile(sparkinput)
      .map(_.split(",", -1))
      .filter(_.length >= 85)
    //数据筛选后将数据放入
    val rddd = value.map(LogBean(_))
    //将rdd转换为dateformat
    val df = spark.createDataFrame(rddd)

    //打印并关闭资源
    df.write.parquet(sparkoutput)

    sc.stop()
    spark.stop()




  }
}
