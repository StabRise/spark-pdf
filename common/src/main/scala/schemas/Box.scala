package com.stabrise.sparkpdf
package schemas

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.unsafe.types.UTF8String

case class Box(text: String,
               score: Float,
               x: Int,
               y: Int,
               width: Int,
               height: Int
              ) {
  def toInternalRow: InternalRow = {
    InternalRow(
      UTF8String.fromString(text),
      score,
      x,
      y,
      width,
      height)
  }
}


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

case class BoxWithMeta(
                        text: String,
                        score: Float,
                        x: Int,
                        y: Int,
                        width: Int,
                        height: Int,
                        isStartWord: Boolean,
                        isStartLine: Boolean
                      ) {
  def toBox(): Box = {
    Box(
      text = text,
      score = score,
      x = x,
      y = y,
      width = width,
      height = height
    )
  }
}
