package com.stabrise.sparkpdf
package schemas

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, Row}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable

case class Document(path: String,
                    text: String,
                    outputType: String,
                    bBoxes: Array[Box],
                    exception: String = ""
                    ) {
  def toInternalRow: InternalRow = {
    val arrayDataBoxes = ArrayData.toArrayData(bBoxes.map(_.toInternalRow))
    InternalRow(
      UTF8String.fromString(path),
      UTF8String.fromString(text),
      UTF8String.fromString(outputType),
      arrayDataBoxes,
      UTF8String.fromString(exception))
  }
}

object Document {
  val columnSchema: StructType = Encoders.product[Document].schema

  def apply(row: Row): Document = {
    Document(
      row.getString(0),
      row.getString(1),
      row.getString(2),
      row.getAs[mutable.WrappedArray[Row]](3).toArray.map(Box(_)),
      row.getString(4),
    )
  }
}
