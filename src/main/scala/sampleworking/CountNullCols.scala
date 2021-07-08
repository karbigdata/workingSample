package sampleworking

import org.apache.spark.sql.functions.{col, count, when}
import org.apache.spark.sql.{Column, SparkSession}

object CountNullCols {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[2]").getOrCreate()

    val dataDF = spark.read.option("inferSchema","true").option("header","true")
      .csv("src/main/resources/data/allAboutScala/nullRows.txt")
    dataDF.show()

    def countNulls(columns:Array[String]) :Array[Column] ={
      columns.map(c => {
        count(when(col(c).isNull ||
                  col(c).contains("null") ||
                  col(c).contains("NULL"), c)).alias(c)
      })
    }
    dataDF.select(countNulls(dataDF.columns):_*).show()


 /*   val data = Seq(("","CA"), ("Julia",null),("Robert",null),(null,""))
    import spark.sqlContext.implicits._
    val df = data.toDF("name","state")
    df.show(false)

    def countCols(columns : Array[String]):Array[Column] ={
      columns.map(c => {
        println("c is " + c)
        count(when(col(c).isNull ||
          col(c) === "" ||
        col(c).contains("NULL") ||
        col(c).contains("null"),c)).alias(c)
      })
    }

    println("df.cols is " + df.columns)
    df.select(countCols(df.columns):_*).show()*/



  }

}
