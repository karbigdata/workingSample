package sampleworking
import org.apache.spark.sql.SparkSession

object JoinsDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    var leftDF = spark.read.option("header",true).csv("src/main/resources/data/firstTable.txt")
    var rightDF = spark.read.option("header",true).csv("src/main/resources/data/secondTable.txt")
    leftDF.show()
    rightDF.show()

    //leftDF.except(rightDF).show()
    //leftDF.intersect(rightDF).show()
    leftDF.unionAll(rightDF).show()
    leftDF.unionAll(rightDF).dropDuplicates().show()

    //leftDF.unionAll(rightDF).except(leftDF.intersect(rightDF)).show()
    /*

    val innerDF = leftDF.join(rightDF, leftDF.col("id") === rightDF.col("id"), "inner")
    innerDF.show()
    val outerDF = leftDF.join(rightDF, leftDF.col("id") === rightDF.col("id"), "outer")
    outerDF.show()
    val leftSemiDF = leftDF.join(rightDF, leftDF.col("id") === rightDF.col("id"), "leftsemi")
    leftSemiDF.show()
    val leftAntiDF = leftDF.join(rightDF, leftDF.col("id") === rightDF.col("id"), "leftanti")
    leftAntiDF.show()
    */

  }

}
