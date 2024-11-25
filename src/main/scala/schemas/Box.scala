package com.stabrise.sparkpdf
package schemas

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, Row}

case class Box(text: String,
               score: Float,
               x: Int,
               y: Int,
               width: Int,
               height: Int
              )


object Box {
  val columnSchema: StructType = Encoders.product[Box].schema

  def apply(row: Row): Box = {
    Box(
      row.getString(0),
      row.getFloat(1),
      row.getInt(2),
      row.getInt(3),
      row.getInt(4),
      row.getInt(5)
    )
  }
}
