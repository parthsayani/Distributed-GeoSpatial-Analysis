package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART

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

    //return true // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
