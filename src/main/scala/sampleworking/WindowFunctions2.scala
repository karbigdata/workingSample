package sampleworking

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Column, SaveMode, SparkSession, functions}
import org.apache.spark.sql.functions.{col, max}

object WindowFunctions2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("spark catalog table").master("local[2]").getOrCreate()
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

    val df = simpleData.toDF("employee_name", "department", "salary")

    //def max(//column: Column) = ???

    val winSpec = Window.partitionBy("department").orderBy()
    val resDF = df
      .groupBy("department").agg(max("salary").as("max"))
      //.withColumn("max_sal_per_dept",max("salary").over(winSpec))

    resDF.show()
/*    resDF.repartition(1).write.csv("/tmp/csv")
    resDF.repartition(1).write.save("/tmp/csv1")*/
  //  resDF.write.mode(SaveMode.Overwrite).saveAsTable("csvTable1")
    spark.sql("CREATE DATABASE IF NOT EXISTS karDB")
    spark.catalog.setCurrentDatabase("karDB")
    resDF.write.mode(SaveMode.Overwrite).saveAsTable("csvTable3")
    spark.catalog.listTables().show()
    spark.sql("select * from csvTable3").show()

  }
}
