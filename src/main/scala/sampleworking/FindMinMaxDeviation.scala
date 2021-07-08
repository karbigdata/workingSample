package sampleworking

import org.apache.spark.sql.SparkSession

object FindMinMaxDeviation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Temp deviation").master("local[2]").getOrCreate()
    val rawData = Seq(
      ("w1",23,20,19,30),("w2",25,21,23,20),("w3",20,23,19,26),("w4",24,21,23,22),("w5",20,19,24,21),("w6",18,17,16,15))
    val thresholdVal = 19
    val cols = Seq("warehouse","ez","wz","sz","nz")
    import spark.implicits._
    val dataDF = rawData.toDF(cols:_ *)
    dataDF.show()
    val colsLength = dataDF.columns.length
    val columnsAvail = dataDF.columns
    var zoneData = ""
    println("cols length is " + colsLength)
    var maxDev:Int = 0
    val dataDF2 = dataDF.map(row => {
      val wsName:String = row(0).asInstanceOf[String]
      maxDev = thresholdVal
      for(i <- 1 until colsLength){
        if(maxDev < row(i).asInstanceOf[Int]){
          maxDev = row(i).asInstanceOf[Int]
          zoneData = columnsAvail(i)
        }
      }
      ( wsName,maxDev,zoneData)
    })
    dataDF2.show()

  }

}
