import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object sql {
  def main(args: Array[String]): Unit = {

    val sparksql = new SparkConf().setMaster("local[*]").setAppName("sparksql")
    val session = SparkSession.builder().config(sparksql).getOrCreate()
    session.read.json("date/a.json")

  }

}
