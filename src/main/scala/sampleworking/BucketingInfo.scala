package sampleworking

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType, TimestampType}

object BucketingInfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("bucketing").master("local[2]").getOrCreate()
    val mySchema = StructType(List(
      StructField("id",IntegerType),
      StructField("firstname",StringType),
      StructField("lastname",StringType),
      StructField("email",StringType),
      StructField("email2",StringType),
      StructField("dateofJoin",TimestampType),
    ))
    val df1 = spark.read
      .schema(mySchema)
      .option("header","true").csv("src/main/resources/data/randomCsvDate.csv")
    df1.show()
    df1.printSchema()
    val df2 = df1.withColumn("year", functions.year(col("dateofJoin")))
      .withColumn("monthOfYear", functions.month(col("dateofJoin")))
    df2.show()

    val df3 = df2.groupBy("year").count()
    df3.explain(true)

    df3.show()
    df3.explain(true)

/*    df2.filter(col("dateofJoin").like("2015-01%")).show()
    println(df2.filter(col("dateofJoin").like("2015-01%")).count())

   // df2.write.partitionBy("year","monthOfYear").csv("/tmp/PARTDATA")

    df2.write.format("csv").bucketBy(5, "year")
      .option("path","/tmp/bucketData2").saveAsTable("bucketedTable2")*/



  }

}
