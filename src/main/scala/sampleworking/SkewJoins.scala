package sampleworking

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, lag, lead, row_number, sum}

import scala.collection.mutable.ListBuffer

case class lineRow(a:Int, b:String)
object SkewJoins {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()

    val dataDF = spark.read.csv("src/main/resources/data/people-1m/people-1m.txt")
    //dataDF.show(10,false)

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    import spark.implicits._
    var df = simpleData.toDF("employee_name", "department", "salary")
    df.show()
    val windowSpec = Window.partitionBy("department").orderBy("salary")
    df = df.withColumn("row_number", row_number.over(windowSpec))

    df = df.withColumn("lead", lead(col("salary"),2).over( windowSpec))
    df = df.withColumn("lag", lag(col("salary"),2).over( windowSpec))
    df.show()

/*    val windowSpecAgg = Window.partitionBy("department")
    val aggDF = df.withColumn("row",row_number.over(windowSpec))
      .withColumn("avg", avg(col("salary")).over(windowSpecAgg))
      .withColumn("sum", sum(col("salary")).over(windowSpecAgg))
      .withColumn("min", functions.min(col("salary")).over(windowSpecAgg))
      .withColumn("max", functions.max(col("salary")).over(windowSpecAgg))
      .where(col("row") === 1)
    aggDF.show()*/







  }

}
