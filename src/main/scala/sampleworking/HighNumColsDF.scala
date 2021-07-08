package sampleworking
import org.apache.spark.sql.SparkSession
import java.io.File
object HighNumColsDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("highest num of cols DF").master("local[2]").getOrCreate()
    val numFileInDir = new File("src/main/resources/diffCols").listFiles
    numFileInDir.foreach(println)
    println("num of files " + numFileInDir.length)
    var maxFileNumCols:Int = 0
    var fileName :String =""
    for(i <- 0 until numFileInDir.length){
      val currFileNumCols = spark.read.option("header","true").csv(numFileInDir(i).toString).columns.length
      println("num of cols " + currFileNumCols)
      if(currFileNumCols > maxFileNumCols){
        maxFileNumCols = currFileNumCols
        fileName = numFileInDir(i).toString
        println(s"fileName is $fileName and cols length is $maxFileNumCols")
      }
    }
    //Now create DF with that fileName and will have file/DF with max columns in a given directory
    val maxColDataDF = spark.read.option("header","true").csv(fileName.toString)
    maxColDataDF.show()
  }
}
