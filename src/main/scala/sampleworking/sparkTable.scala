package sampleworking

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
case class myRowIs(empName:String,dept:String,city:String,salary:Int, age:Int, bonus:Int)
object sparkTable {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark catalog table").master("local[2]").getOrCreate()
    val simpleData2 = Seq(("James","Sales","NY",90000,34,10000),
      ("Maria","Finance","CA",90000,24,23000),
      ("Jen","Finance","NY",79000,53,15000),
      ("Jeff","Marketing","CA",80000,25,18000),
      ("Kumar","Marketing","NY",91000,50,21000)
    )
    import spark.implicits._
    val dF1= simpleData2.toDF("empName","dept","city","salary","age","bonus")
    println("dF1 is  " + dF1.getClass)
    dF1.show()
    val ds1 = dF1.as[myRowIs]
    println("ds1 is " + ds1.getClass)
    ds1.show()

    val df2 = ds1.toDF()




  }

}
