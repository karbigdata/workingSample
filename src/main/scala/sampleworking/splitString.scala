package sampleworking

import org.apache.spark.sql.SparkSession

case class myRow(fName:String, lName:String)
object splitString {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    var df1 = spark.read.option("header",true).csv("src/main/resources/data/allAboutScala/splitString.txt")
    df1.show()
    import spark.implicits._
    val df2 = df1.map(row => {
      val arrStr = row.mkString.split(" ")
      myRow(arrStr(0),arrStr(1))
    })
    df2.show()

  }

}
