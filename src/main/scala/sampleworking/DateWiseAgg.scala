package sampleworking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, col, collect_list, date_format, grouping, lit, monotonically_increasing_id, sum, to_date, when}

object DateWiseAgg {
  def main(args: Array[String]): Unit = {

    val rawData = Seq(("05/01/2021", "A", 400), ("15/01/2021", "A", 300),
      ("06/01/2021", "A", 700), ("10/01/2021", "A", 100),
      ("12/01/2021", "B", 300), ("14/01/2021", "B", 200))
    val spark = SparkSession.builder().appName("date wise agg").master("local[2]").getOrCreate()
    import spark.implicits._
    val dataDF = rawData.toDF("date", "comm_col", "amt")
    dataDF.show()
    dataDF.printSchema()
    dataDF.sort("date").show()

    dataDF.createOrReplaceTempView("df_table")

    val resDF = dataDF.withColumn("identify", when(col("date") <= "10/01/2021", lit("10/01/2021"))
    .otherwise(col("date").as("new_date"))).drop("date")
    resDF.show()

    val resDF1 =  resDF.groupBy("identify","comm_col").agg(sum("amt"))
    resDF1.show()




  }
}
