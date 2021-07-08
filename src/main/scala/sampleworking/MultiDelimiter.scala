package sampleworking

import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}

case class newCS(id:Int, strVal:String, amt:Long)
object MultiDelimiter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Multi delim").master("local[2]").getOrCreate()
    val dataDF = spark.read
      .text("src/main/resources/data/allAboutScala/MultiDelimiter.csv")
    dataDF.printSchema()
    dataDF.show()
    val replaceChars = ",$@# "
    val df2 = dataDF.withColumn("col1", regexp_replace(col("value"), replaceChars, "|"))
    df2.show()
    import spark.implicits._
    spark.read.option("delimiter", "|").csv(df2.map(x => x.getString(0))).show()



/*    var dataDFRes = spark.emptyDataFrame
    import spark.implicits._
    val dataDF2 = dataDF.map(row => {
      val rowStr = row.mkString
     // println("rowStr is " + rowStr)
      val newRowStr = rowStr.replaceAll("[ $@#&]",",")
    //  println("new Str is " + newRowStr)
      val arrayOfStr = newRowStr.split(",")
      val itrColLength = newRowStr.split(",").length
      newCS(arrayOfStr(0).toInt, arrayOfStr(1),arrayOfStr(2).toLong)
      //(arrayOfStr(0),arrayOfStr(1),arrayOfStr(2)
    })

    dataDF2.printSchema()
    dataDF2.show()*/

/*

    var itrColLen = 0
    import spark.implicits._
    val dataDF3 = dataDF.map(row => {
      val myStrDelim = row.mkString
      val myStrWODelim = myStrDelim.replaceAll("[ $@#&]", ",")
      itrColLen = myStrWODelim.split(",").length



      myStrWODelim
    })
    println("length is " + itrColLen)

    var df3 = dataDF3.withColumn("value", functions.split(col("value"),","))
    val df4 = df3.select( (0 until itrColLen).map( i => col("value").getItem(i).as("col_" + i): _*))
    df3.printSchema()
    df3.show()
*/























  }

}
