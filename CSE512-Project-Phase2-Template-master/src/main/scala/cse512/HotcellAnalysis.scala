package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()


  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART

  pickupInfo.createOrReplaceTempView("pickupInfoView")

  // Removing the noise by using the min max of x,y,z as defined above
  var query =
    s"""
        SELECT x, y, z
        FROM pickupInfoView
        WHERE x BETWEEN $minX AND $maxX
        AND y BETWEEN $minY AND $maxY
        AND z BETWEEN $minZ AND $maxZ
        ORDER BY z, y, x
    """
  pickupInfo = spark.sql(query)
  pickupInfo.createOrReplaceTempView("filteredPickUpInfo")
  pickupInfo.show()

  // Calculating each cell hotness
  query =
    """
       SELECT x, y, z, COUNT(*) AS hotness
       FROM filteredPickUpInfo
       GROUP BY x, y, z
       ORDER BY z, y, x
    """
  val cellHotness = spark.sql(query)
  cellHotness.createOrReplaceTempView("cellHotness")
  cellHotness.show()

  // Calculating total hotness
  query =
    s"""
        SELECT SUM(hotness) AS totalHotness
        FROM cellHotness
    """
  val totalCellHotness = spark.sql(query)
  totalCellHotness.createOrReplaceTempView("totalCellHotness")
  totalCellHotness.show()

  val mean = totalCellHotness.first().getLong(0).toDouble/numCells.toDouble
  println(mean)

  query =
    s"""
        SELECT SUM(hotness*hotness) AS sumOfSquareHotness
        FROM cellHotness
    """
  val sumOfSquareHotness = spark.sql(query)
  sumOfSquareHotness.createOrReplaceTempView("sumOfSquareHotness")
  sumOfSquareHotness.show()

  val std = scala.math.sqrt(sumOfSquareHotness.first().getLong(0).toDouble/numCells.toDouble - mean*mean)
  println(std)

  spark.udf.register("noOfAdjacentCells",
    (inputX: Int, inputY: Int, inputZ: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int)
    =>
      HotcellUtils.noOfAdjacentCells(inputX, inputY, inputZ, minX, maxX, minY, maxY, minZ, maxZ)
  )

  query =
    s"""
        SELECT
          noOfAdjacentCells(self1.x, self1.y, self1.z, $minX, $maxX, $minY, $maxY, $minZ, $maxZ) AS adjacentCells,
          self1.x x,
          self1.y y,
          self1.z z,
          SUM(self2.hotness) AS adjacentTotalHotness
        FROM cellHotness self1, cellHotness self2
        WHERE self2.x IN (self1.x - 1, self1.x, self1.x + 1)
        AND self2.y IN (self1.y - 1, self1.y, self1.y + 1)
        AND self2.z IN (self1.z - 1, self1.z, self1.z + 1)
        GROUP BY self1.z, self1.y, self1.x
        ORDER BY self1.z, self1.y, self1.x
    """
  val adjacentCellHotness = spark.sql(query)
  adjacentCellHotness.createOrReplaceTempView("adjacentCellHotness")
  adjacentCellHotness.show()

  spark.udf.register("zScore",
    (adjacentCellCount: Int, sumHotCells: Int, numCells: Int, x: Int, y: Int, z: Int, mean: Double, standardDeviation: Double)
    =>
      HotcellUtils.zScore(adjacentCellCount, sumHotCells, numCells, x, y, z, mean, standardDeviation)
  )

  query =
    s"""
       SELECT zScored.x x, zScored.y y, zScored.z z FROM (
         SELECT zScore(adjacentCells, adjacentTotalHotness, $numCells, x, y, z, $mean, $std) AS Getis_Ord_Statistic, x, y, z
         FROM adjacentCellHotness
         ORDER BY Getis_Ord_Statistic DESC
       ) zScored
    """
  val zScoreOrdered = spark.sql(query)
  zScoreOrdered.show()


  //////////////////////////

  zScoreOrdered // YOU NEED TO CHANGE THIS PART
}
}
