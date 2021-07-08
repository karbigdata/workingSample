package sampleworking

import org.apache.spark.sql.SparkSession

object InitCapital extends App{
  val spark = SparkSession.builder().appName("init capital").master("local[2]").getOrCreate()
  val data = Seq(("virat kohli karthik"),("monalisha victor"),("iron man"),("avenger thor"))
  import spark.implicits._
  val dataDF = data.toDF("col1")
  dataDF.printSchema()
  dataDF.show()
  val df2 = dataDF.map(row => {
    println("row is " + row.mkString)
    var arrStr = row.mkString.split(" ")
    //assuming that first string will have 2 init capitals and rest with 1 init capital
    for( i <- 0 until arrStr.length) {
      if(i == 0) {
        arrStr(i) = arrStr(i).substring(0, 2).toUpperCase  + arrStr(i).substring(2)
      }
      else {
        arrStr(i) = arrStr(i).substring(0, 1).toUpperCase  + arrStr(i).substring(1)
      }
    }
    arrStr.mkString(" ")
  })
  df2.show(false)
}
