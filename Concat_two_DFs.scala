  import spark.implicits._
    val df1 = spark.sparkContext.parallelize(Seq(
      (1, "abc"),
      (2, "def"),
      (3, "hij")
    )).toDF("id", "name")

    val df2 = spark.sparkContext.parallelize(Seq(
      (19, "x"),
      (29, "y"),
      (39, "z")
    )).toDF("age", "address")

    val schema = StructType(df1.schema.fields ++ df2.schema.fields)

    val df1df2 = df1.rdd.zip(df2.rdd).map{
      case (rowLeft, rowRight) => Row.fromSeq(rowLeft.toSeq ++ rowRight.toSeq)}

    val dataDF2 = spark.createDataFrame(df1df2, schema)

    dataDF2.show()
    dataDF2.printSchema()
