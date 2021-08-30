import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object SaltingTech {
  val conf = new SparkConf().set("spark.sql.shuffle.partitions", "3")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
  val spark = SparkSession.builder().appName("Salting Tech").config(conf)
    .master("local[2]").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def eliminateDataSkew(leftDF:DataFrame, leftCol:String, rightDF:DataFrame) ={

    var df1 = leftDF
      .withColumn(leftCol,functions.concat(leftDF.col(leftCol), lit("_"), lit(floor(rand(123456)*3))))
    df1.show()

    val df2 = rightDF
      .withColumn("explodedCol",
        explode(functions.array((0 to 3).map(lit(_)):_*)))

    df2.show()
    (df1,df2)
  }
  def removeSkewUsingSQL(leftDF:DataFrame, rightDF:DataFrame) ={
    println("Inside spark sql skew data removal ")

    leftDF.createOrReplaceTempView("leftDF")
    rightDF.createOrReplaceTempView("rightDF")

    val salted_fact_df = spark.sql("select " +
      " concat(col1, '_', FLOOR(RAND(123456)*19))" +
      "as SALTED_KEY, col2 from leftDF")
    salted_fact_df.show()

    salted_fact_df.createOrReplaceTempView("salted_fact_df")

    val exploded_dim_df = spark.sql("select col1, col2, " +
      " explode(array(0,10)) " +
      "as salted_key from rightDF")

    exploded_dim_df.show()
    exploded_dim_df.createOrReplaceTempView("exploded_dim_df")

/*    spark.sql("select count(*) from" +
      "( select split(t1.SALTED_KEY,'_')[0] as key1 " +
      "from salted_fact_df t1, exploded_dim_df t2 " +
      "where t1.SALTED_KEY = concat(t2.col1,'_',t2.salted_key))").show()
      /*"group by col1 " +
      "order by col1").show()*/*/

    salted_fact_df.join(exploded_dim_df,
      salted_fact_df.col("SALTED_KEY") === concat(exploded_dim_df.col("col1"),
        lit("_"),exploded_dim_df.col("salted_key"))).show()




  }
  def main(args: Array[String]): Unit = {

    var leftDF = spark.read
      .option("inferSchema","true")
      .option("header","true")
      .csv("src/main/resources/saltingData/file11.csv")

    //leftDF.printSchema()

    leftDF = leftDF.union(leftDF)
    leftDF = leftDF.repartition(3)

    println("Num of partitions are " + leftDF.rdd.getNumPartitions)

    val rightDF = spark.read
      .option("inferSchema","true")
      .option("header","true")
      .csv("src/main/resources/saltingData/file12.csv").repartition(3)

/*    val countDFTab1 = leftDF.select(col("col1")).groupBy("col1").count()
    countDFTab1.show()

    val countTab2DF = rightDF.select(col("col1")).groupBy("col1").count()
    countTab2DF.show()
*/
/*    val joinedDF = leftDF.join(rightDF, leftDF.col("col1") === rightDF.col("col1"), "inner")
    joinedDF.show()
    joinedDF.explain(true)*/

  //  joinedDF.explain(true)


    // remove skewness from the data
    var(df3,df4) =  eliminateDataSkew(leftDF,"col1", rightDF)

    df3.join(df4,
      df3.col("col1") ===  concat(df4.col("col1"),lit("_"),
      df4.col("explodedCol")))
      .show()

   // var sparkSqlTempDF = removeSkewUsingSQL(leftDF, rightDF)

    println("Enter DONE !!!")
    val exitCode = System.in.read()
    println("pgm is existed " + exitCode)

  }

}
