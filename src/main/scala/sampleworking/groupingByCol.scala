package sampleworking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, countDistinct, expr, round, sum, to_date, weekofyear}

object groupingByCol {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    var df1 = spark.read.option("header",true).csv("src/main/resources/data/allAboutScala/invoices.csv")
    df1.show(10)

    val df2 = df1.withColumn("InvoiceDate", to_date(col("InvoiceDate"), "dd-MM-yyyy H.mm"))
      .withColumn("WeekNum", weekofyear(col("InvoiceDate")))
      .groupBy("Country","WeekNum")
      .agg(countDistinct("InvoiceNo").as("NumInvoices"),
        sum("Quantity").as("TotalQuantity"),
        round(sum(expr("Quantity * UnitPrice")),2).as("InvoiceValue"))

   /* df2.coalesce(1)
      .write
      .format("parquet")
      .mode("overwrite")
      .save("/tmp/karParquet")*/

   // df2.sort(col("Country"), col("WeekNum").desc).show()
  //  df2.show()
  //  df2.printSchema()

    val windowFunc = Window.partitionBy("Country").orderBy("WeekNum").rowsBetween(Window.unboundedPreceding,Window.currentRow)
    df2.withColumn("RunningTotal", sum("InvoiceValue").over( windowFunc))
      //.where(col("Country") === "France").sort(col("WeekNum").desc)
      .show()




  }

}
