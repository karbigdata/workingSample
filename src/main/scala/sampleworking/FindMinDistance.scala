package sampleworking

import org.apache.spark.sql.{Dataset, Row, SparkSession, functions}

case class minD(pId:Int, price:Int, minD:Int)
object FindMinDistance {
  val spark = SparkSession.builder().appName("Min distance").master("local[2]").getOrCreate()
    def getMinDistance(row: Row, newPriceList: List[Int]): minD = {
      val oldPrice: Int = row(1).asInstanceOf[Int]
      var minDist = 9999
      println("old price is " + oldPrice)
      newPriceList.foreach(x => {
        val currDist = Math.abs(oldPrice - x)
        if (currDist < minDist) {
          minDist = currDist
        }
      })
      minD(row(0).asInstanceOf[Int], row(1).asInstanceOf[Int], minDist)
  }
  def main(args: Array[String]): Unit = {
    val rawData = Seq((101,122,120),(102,90,95),(103,110,111),(104,100,121))
    val cols = Seq("prod_id", "price","newPriceList")
    import spark.implicits._
    val dataDF = rawData.toDF(cols:_ *)
    dataDF.show()
    dataDF.printSchema()
    val newDataDF = dataDF.select("prod_id","price")
    val newPriceList = dataDF.select("newPriceList").rdd.map(r => r(0)).collect().toList

    val resultDF = newDataDF.map(row => {
    val minDistanceTup = getMinDistance(row, newPriceList.asInstanceOf[List[Int]])
      println("distance is " + minDistanceTup)

      minDistanceTup
    })
    resultDF.show()
   }

}
