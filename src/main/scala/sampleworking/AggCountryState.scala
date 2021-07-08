package sampleworking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, col, collect_list, explode, sum, when}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object AggCountryState {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("agg country state").master("local[2]").getOrCreate()
    val mySch = StructType(List(
      StructField("country",StringType),
      StructField("city",StringType),
      StructField("value",IntegerType),
    ))
    val dataDF = spark.read
      .schema(mySch).option("header","true")
      .csv("/home/apple/Documents/sparkPractice/src/main/resources/data/Country_State_Input")

    dataDF.show()
    dataDF.printSchema()

/*    //conditional aggregates for India and Pune
    //pune only DF
    val puneOnlyDF = dataDF.filter(col("country") === "India" and col("city") === "Pune")
    puneOnlyDF.show()
    val notPuneOnlyDF = dataDF.filter(col("country") =!= "India" or col("city")  =!= "Pune")
    notPuneOnlyDF.show()

    val puneOnlyDFAgg = puneOnlyDF.groupBy("country", "city").agg(sum("value"))
    val resDF = puneOnlyDFAgg.unionAll(notPuneOnlyDF)
    resDF.show()*/

    val res1DF = dataDF.groupBy("country","city").agg(when(col("city") === "Pune", array(sum("value")))
      .otherwise(collect_list("value")).as("value"))
     // .withColumn("value", explode(col("value")))
    res1DF.show()




  }

}
