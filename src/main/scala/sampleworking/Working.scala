package sampleworking

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, dense_rank, lag, rank, row_number, to_date}
case class Salary(depName: String, empNo: Long, salary: Long)
object Working {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()
    var df1 = spark.read.option("header",false).csv("src/main/resources/data/allAboutScala/DFWrite.csv/part-00000-adef9ad1-d283-45e5-b394-db54dba1b0c1-c000.csv")
    df1.show()
    df1.createOrReplaceTempView("table1")
    val df4 = df1.select(col("*"))
      .groupBy(col("_c1"),col("_c0")).count().filter(col("_c0") === "xyz")
    df4.show()
    df4.explain(extended = true)

    val df5 = spark.sql("""select _c0,_c1, count(_c1) from table1 group by _c1, _c0 having _c0 = "xyz" """)
    df5.show()
    df5.explain(extended = true)

    /*
    df1 = df1.withColumn("datetime", to_date(col("datetime"),"yyyyMMdd"))

    val windowSpec = Window.partitionBy("loc").orderBy(col("datetime").desc)
    val df2 = df1.withColumn("row_num", row_number().over(windowSpec)).where(col("row_num") === 1).drop("row_num")
    val df3 = df2.unionAll(df2).unionAll(df2)
    df3.repartition(1).write.csv("src/main/resources/data/allAboutScala/DFWrite.csv")*/
















/*    val rdd1 = spark.sparkContext.textFile("src/main/resources/data/allAboutScala/abc.txt")
    //rdd1.foreach(println)
    val rdd2 = rdd1.map(lines => {
      var line = lines.split("\n")
      line.filter(l => line.contains("data"))
      line.mkString
    })
    rdd2.foreach(println)*/


  }

}
