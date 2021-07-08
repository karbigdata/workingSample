package sampleworking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import java.io.File
import scala.reflect.internal.util.Collections
import scala.util.control.Breaks.{break, breakable}

object SchemaCheck {
  def main(args: Array[String]): Unit = {
    var count = 0;
    var flag = false
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    val dirLoc = new File("src/main/resources/data/sampleFiles").listFiles
    dirLoc.map(x => count = count + 1)
    println("num of files in dirLoc is " + count)
    var previousDFSchemaCount: Int = 0
    var currentDFSchemaCount: Int = 0
    val df0 = spark.read.option("inferSchema", "true")
      .csv(dirLoc(0).toString).columns.length
    println("num of cols are " + df0)
   /* previousDFSchemaCount =
    df0.printSchema()
    breakable {
      for (i <- 1 to count - 1) {
        var dfs = spark.read.option("inferSchema", "true").csv(dirLoc(i).toString)
        currentDFSchemaCount = dfs.schema.length
        if (currentDFSchemaCount.equals(previousDFSchemaCount)) {
          flag = true
          previousDFSchemaCount = currentDFSchemaCount
        }
        else {
          flag = false
          println("oops this file has diff schema " + dirLoc(i).toString)
          //currentDFSchemaCount = previousDFSchemaCount
          break
        }
      }
    }
    if(!flag){
      println("Schema not Matched !!!")
    }else{
      println("All files schema Matched !!!")
    }*/
  }
}





