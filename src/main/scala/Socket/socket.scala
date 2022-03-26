package Socket

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object socket {
  def main(args: Array[String]): Unit = {
    //首先
    val unit = Logger.getLogger("org").setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("Spark shell")
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext
    val lien = spark.readStream.format("socket").option("cc", "192.168.43.133").option("port",8000).load()
    val rr = lien.as[String].flatMap(_.split(" "))




  }
}
