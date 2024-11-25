package com.stabrise.sparkpdf
package schemas

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, Row}

import scala.collection.mutable

case class Document(path: String,
                    text: String,
                    outputType: String,
                    bBoxes: Array[Box],
                    exception: String = ""
                    )

object Document {
  val columnSchema: StructType = Encoders.product[Document].schema

  def apply(row: Row): Document = {
    Document(
      row.getString(0),
      row.getString(1),
      row.getString(2),
      row.getAs[mutable.WrappedArray[Box]](3).toArray,
      row.getString(4),
    )
  }
}
