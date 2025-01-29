package com.stabrise.sparkpdf
package datasources

import com.stabrise.sparkpdf.DefaultOptions
import com.stabrise.sparkpdf.ocr.TesseractBytedeco
import com.stabrise.sparkpdf.schemas.{Box, Document}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.nio.file.Paths


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
    val forceOCR = options.getOrElse("forceOCR", "false").toBoolean

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
    val ocrDocument = if (readDataSchema.fieldNames.contains("document")) {
      if (forceOCR || text.isEmpty) {
        tesseract.imageToDocument(image, PIL.WORD, 0, filename)
      } else {
        Document(filename, "", "", Array[Box]())
      }
    } else
      Document(filename, "", "", Array[Box]())

    // Assemble final row
    InternalRow(
      UTF8String.fromString(filename),
      UTF8String.fromString(Paths.get(filename).getFileName.toString),
      pageNumCur,
      inputPartition.index,
      UTF8String.fromString(text),
      imageRow,
      ocrDocument.toInternalRow
    )
  }
}
