package YTube_InterviewScenarios

import org.apache.spark
import org.apache.spark.sql.functions.{col, explode, json_tuple}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object ArrayExplode {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("array explode").master("local[2]").getOrCreate()
    val dataDF = spark.read
      .option("header", "true")
      .option("multiline","true")
      .option("escape", "\"")
      .option("sep", ",")
      .csv("src/main/resources/dir2/*.csv")

    var dataDF2 = dataDF.select(col("*"), json_tuple(col("request"),"Response").alias("response")).drop("request")
    var dataDF3 = dataDF2.select(col("*"),json_tuple(col("response"),"MessageId","Latitude","longitude")).drop("response")
    dataDF3.show(false)
  //  dataDF3.printSchema()

    dataDF2.show(false)
  }

}
