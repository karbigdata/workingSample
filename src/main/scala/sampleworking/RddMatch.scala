package sampleworking

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.util.control.Breaks
import scala.util.control.Breaks.{break, breakable}

object RddMatch {
  def rddCheck(fLine:String, rdd2:RDD[String]) : Boolean ={
    var tempFl = false
    rdd2.map(line =>{
      println("fLine and currLine " + fLine + " "+ line)
      if(line.equals(fLine)){
        println("inside equals")
        tempFl = false
        break
      }else{
        println("else cond")
        tempFl = true
      }

    })
    tempFl
  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    val rddData = spark.sparkContext.textFile("src/main/resources/data/duplicates.txt")
    rddData.foreach(println)
    val rdd2 = spark.sparkContext.textFile("src/main/resources/data/duplicates.txt")
    rdd2.foreach(println)
    var tempFlag = false
      val resRdd = rddData.map(fLine => {

        val res = rddCheck(fLine, rdd2)
        println(res)
        res

      })
    resRdd.foreach(println)

  }

}
