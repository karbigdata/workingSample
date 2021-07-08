package sampleworking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{aggregate, array, collect_list, struct, sum}

object SumOfCollec {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("agg cols").master("local[2]").getOrCreate()
    val rawData = Seq((50,4),(50,2),(10,3),(20,4),(10,5))
    import spark.implicits._
    val df1 = rawData.toDF("col1","col2")
    df1.show()
    val df2 = df1.groupBy("col1").agg(sum("col2")).alias("aggSum")
    df2.show()
    val df3 = df1.join(df2,df1("col1")===df2("col1"), "inner").drop(df2("col1"))
    df3.show()
    val df4 = df3.select("col1","col2","sum(col2)").withColumn("sumAgg", struct("col2","sum(col2)"))
      .drop("col2","sum(col2)")
    df4.show()



  }

}
