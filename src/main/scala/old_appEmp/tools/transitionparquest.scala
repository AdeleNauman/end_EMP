package old_appEmp.tools

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}

object transitionparquest {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      """
        |缺少参数
        |""".stripMargin
      sys.exit()
    }

    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession
      .builder()
      .config(conf)
      .appName("sparkc")
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    var  Array(inputt,inputoutput)=args
    val txt = sc.textFile(inputt)
    import spark.implicits._
    val map = txt.map {
      _.split(",", -1)
        .filter(_.length >= 85)
    }.toDF()
    println(map)
    map.collect().foreach(println)
//    val edd = sc.makeRDD("")
//    val fmat = spark.read.json(Seq(map))
//    fmat.collect().foreach(println)

  }
}
