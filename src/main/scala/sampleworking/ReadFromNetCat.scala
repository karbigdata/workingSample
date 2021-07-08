package sampleworking

import org.apache.spark.sql.SparkSession

object ReadFromNetCat {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("net cat").master("local[2]").getOrCreate()
    val ncDF = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","12345").load

    println("class is " + ncDF.getClass)

    val linesDF = ncDF.select("*")

    linesDF.writeStream.format("console").outputMode("update").start().awaitTermination()

  }

}
