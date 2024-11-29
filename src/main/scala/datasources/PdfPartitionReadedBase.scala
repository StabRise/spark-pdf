package com.stabrise.sparkpdf
package datasources

import ocr.TesseractBytedeco
import schemas.Box
import java.nio.file.Paths
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.catalyst.util.ArrayData

abstract class PdfPartitionReadedBase(inputPartition: FilePartition,
                                      readDataSchema: StructType,
                                      options: Map[String,String])
  extends PartitionReader[InternalRow] {

  var filename: String = ""
  lazy val tesseract = new TesseractBytedeco(config = options.getOrElse("ocrconfig", DefaultOptions.OCR_CONFIG))
  var pageNumCur: Int = 0


  def getSearchableText: String = ""

  def renderImage(resolution: Int): Array[Byte]

  override def get(): InternalRow = {
    val resolution = options.getOrElse("resolution", DefaultOptions.RESOLUTION).toInt

    val text = if (readDataSchema.fieldNames.contains("text")) {
      getSearchableText
    } else ""

    // Render the image from the PDF
    val image = if (readDataSchema.fieldNames.contains("image") || readDataSchema.fieldNames.contains("document")) {
      renderImage(resolution)
    } else Array[Byte]()

    val imageRow = InternalRow(
      UTF8String.fromString(filename),
      resolution,
      image,
      UTF8String.fromString("file"),
      UTF8String.fromString(""),
      0,
      0)

    // Run OCR on the image
    val ocrText = if (readDataSchema.fieldNames.contains("document")) {
      tesseract.imageToText(image)
    } else ""

    val bBoxes = ArrayData.toArrayData(Array.empty[Box])
    val documentRow = InternalRow(
      UTF8String.fromString(filename),
      UTF8String.fromString(ocrText),
      UTF8String.fromString("ocr"),
      bBoxes,
      UTF8String.fromString(""))

    // Assemble final row
    InternalRow(
      UTF8String.fromString(filename),
      UTF8String.fromString(Paths.get(filename).getFileName.toString),
      pageNumCur,
      inputPartition.index,
      UTF8String.fromString(text),
      imageRow,
      documentRow
    )
  }

}
