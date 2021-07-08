package sampleworking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, monotonically_increasing_id}

object Incremental_ID_prob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("incremental id").master("local[2]").getOrCreate()
    var dataDF = spark.read
      .option("header","true").option("inferSchema","true")
      .textFile("/home/apple/Documents/projectsUdemy/sparkStreamingUdemy/udemy-spark-streaming-master/src/main/resources/data/allAboutScala/twoCols3.txt")
    dataDF.show()
   dataDF.withColumn("IncID", lit(1000)+monotonically_increasing_id()).show()
  }

}
