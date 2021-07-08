package sampleworking

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

case class myRow2(id:String, value:String)
object RddDropDuplicates {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    val rddData = spark.sparkContext.textFile("src/main/resources/data/duplicates.txt")
    rddData.foreach(println)
    println("*********")
    val rdd2 = rddData.zipWithIndex()
    rdd2.foreach(println)

    import spark.implicits._
/*    val df = rdd2.toDF()
    df.show()*/
/*

    var resDF = df.map(row => {
      val data = row.mkString.split(",")
      data.foreach(println)
      myRow2(data(0),data(1))
    })
    resDF.show()

    val resDF2 = resDF.select(col("id").cast("integer"),col("value"))
    resDF2.show()
    resDF2.printSchema()
*/
/*

    val mySchema = StructType(List(
      StructField("id",IntegerType),
      StructField("value",StringType)
    ))


    val df1 = spark.read
      .option("header","true")
      .schema(mySchema)
      .csv("src/main/resources/data/duplicates.txt")
    df1.show()

    val rddRow = df1.rdd.mapPartitionsWithIndex((id,x)=> if(id ==0) x.drop(2) else x)
    val schem = StructType(List(
      StructField("id",IntegerType,false),
      StructField("value",StringType,false)
    ))

    val df3 = spark.createDataFrame(rddRow, schem)
    df3.printSchema()
    df3.show()

    //dropping the last 3 lines
    println("dropping last 3 lines")
    val numPart = df3.rdd.getNumPartitions
    val numEle:Long = df3.count()
    val last3Lines:Int = 3
    val df4 = df3.limit((numEle - last3Lines).asInstanceOf[Int] )
   //println("df3.count is " + df4)
    df4.show()
*/

    //val df2 = df1.drop()
  }
}
