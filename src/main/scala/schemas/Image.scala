package com.stabrise.sparkpdf
package schemas

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoders, Row}

case class Image(path: String,
                 resolution: Int,
                 data: Array[Byte],
                 imageType: String,
                 exception: String,
                 height: Int,
                 width: Int
                )

object Image {
  val columnSchema: StructType = Encoders.product[Image].schema

  def apply(row: Row): Image = {
    Image(
      row.getString(0),
      row.getInt(1),
      row.getAs[Array[Byte]](2),
      row.getString(3),
      row.getString(4),
      row.getInt(5),
      row.getInt(6)
    )
  }
}
