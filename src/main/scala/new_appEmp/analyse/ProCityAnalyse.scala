package new_appEmp.analyse

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ProCityAnalyse {
  def main(args: Array[String]): Unit = {
  //1、判断参数是否正确
    if(args.length !=2 ){
      println(
        """
          |缺少参数
          |""".stripMargin
      )
    }

    //使用dateformat的方式
    //先引入环境 Session 需要转换序列化方式
    var conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder()
      .config(conf)
      .appName("log")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //接受参数
    var Array(inputPath,outputPath) = args
    val df:DataFrame = spark.read.parquet(inputPath)
    df.createTempView("log")
    //编写sql语句
    var sql = "select provincename,cityname,count(*)as ct from log group by provincename,cityname"
    //传递sql
    val frame = spark.sql(sql)
    val configuration = sc.hadoopConfiguration
    //文件系统对象
    val fs = FileSystem.get(configuration)
    val path = new Path(outputPath)
    //过滤空数据
    if(fs.exists(path)){
      fs.delete(path,true)
    }
    //打印查询的结果
    frame.coalesce(1).write.partitionBy("provincename","cityname")json(outputPath)
    //关闭资源
    spark.stop()
    sc.stop()

  }
}
