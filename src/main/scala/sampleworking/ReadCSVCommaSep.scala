package sampleworking

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, functions}


object ReadCSVCommaSep {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("comma sep ").master("local[2]").getOrCreate()
    var dataDF = spark.read
      .csv("src/main/resources/commaSepFile.csv")

    dataDF.show()
    dataDF.printSchema()
    val processedDF = dataDF.withColumn("col2", functions.concat(col("_c2"),col("_c3"))).drop("_c2","_c3")
    processedDF.show()



  }

}
