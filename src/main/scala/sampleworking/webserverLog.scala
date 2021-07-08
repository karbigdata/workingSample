package sampleworking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.substring_index

case class ApacheLogRecord(ip: String, date: String, request: String, referrer: String)
object webserverLog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("webserverLog").master("local[2]").getOrCreate()
    val dataDF = spark.read
      //.option("mode", "DROPMALFORMED")
      .textFile("src/main/resources/data/allAboutScala/apache_logs.txt").toDF()

    dataDF.show(10,false)

    //val myReg = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)""".r
    val myReg = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)"""".r

    import spark.implicits._

    val logDF = dataDF.map(row =>
      row.getString(0) match {
        case myReg(ip, client, user, date, cmd, request, proto, status, bytes, referrer, userAgent) =>
          ApacheLogRecord(ip, date, request, referrer)
      }
    )
    logDF.createOrReplaceTempView("logDF")
    logDF.cache()
    logDF.withColumn("referrer", substring_index($"referrer", "/", 3))
      .groupBy("referrer").count().show(false)

    spark.catalog.listTables().show()
  }

}
