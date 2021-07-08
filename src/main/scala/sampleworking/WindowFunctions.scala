package sampleworking
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number

object WindowFunctions {
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
        //df.show()
        val winSpec = Window.partitionBy().orderBy("salary")
        val df2 = df.withColumn("row_num", row_number.over(winSpec))
        df2.show()

        if(df == df2)
          {
            println("true" + true)
          }
          else{
          println("false "  + false)
        }



      }

    }

