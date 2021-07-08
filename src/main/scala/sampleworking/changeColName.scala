package sampleworking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, monotonically_increasing_id, udf}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

object changeColName {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Multi delim").master("local[2]").getOrCreate()
   val dataList = List(
     ("Ravi","28","1","2002"),
     ("Abdul","23","5","81"), //1981
     ("John","12","12","6"),  //2006
     ("Rosy","7","8","63"),  //1963
     ("Abdul","23","5","81"))

    val rawDF = spark.createDataFrame(dataList).toDF("name","day","month","year")
    rawDF.show()

    val finalDF = rawDF.withColumn("id", monotonically_increasing_id())
      .withColumn("year",col("year").cast(IntegerType))
      .withColumn("year", expr(
        """
          |CASE WHEN year < 21 THEN year + 2000
          |WHEN year > 21 and year < 99 THEN year + 1900
          |ELSE year END
          |""".stripMargin))
    finalDF.show()
    //finalDF.printSchema()

    val uniDF = rawDF.distinct()
    uniDF.show()




  }

}
