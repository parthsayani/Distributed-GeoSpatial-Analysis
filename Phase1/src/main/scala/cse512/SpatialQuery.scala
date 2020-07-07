package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def ST_contains(queryRectangle: String, pointString: String): Boolean = {
    val rectPoints = queryRectangle.split(",")
    val rectX0 = rectPoints(0).toDouble
    val rectY0 = rectPoints(1).toDouble
    val rectX1 = rectPoints(2).toDouble
    val rectY1 = rectPoints(3).toDouble

    val pointPoints = pointString.split(",")
    val pointX = pointPoints(0).toDouble
    val pointY = pointPoints(1).toDouble

    if(math.min(rectX0, rectX1) <= pointX && pointX <= math.max(rectX0, rectX1) && math.min(rectY0, rectY1) <= pointY && pointY <= math.max(rectY0, rectY1))
      true
    else
      false
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    //spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((true)))
    spark.udf.register("ST_Contains", (queryRectangle:String, pointString:String) => {
      ST_contains(queryRectangle, pointString)
    })

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=> {
      ST_contains(queryRectangle, pointString)
    })

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def ST_within(pointString1: String, pointString2: String, distance: Double): Boolean = {
    val point1 = pointString1.split(",")
    val point1X = point1(0).toDouble
    val point1Y = point1(1).toDouble

    val point2 = pointString2.split(",")
    val point2X = point2(0).toDouble
    val point2Y = point2(1).toDouble

    if(math.sqrt((point1X - point2X)*(point1X - point2X) + (point1Y - point2Y)*(point1Y - point2Y)) <= distance)
      true
    else
      false
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double) => {
      ST_within(pointString1, pointString2, distance)
    })

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within", (pointString1:String, pointString2:String, distance:Double) => {
      ST_within(pointString1, pointString2, distance)
    })
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
