package com.stabrise.sparkpdf
package datasources

import org.apache.hadoop.conf.Configuration
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.rendering.{ImageType => PDFBoxImageType, PDFRenderer}
import org.apache.pdfbox.text.PDFTextStripper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.io.ByteArrayOutputStream
import javax.imageio.ImageIO

class PdfPartitionReaderPDFBox(inputPartition: FilePartition,
                               readDataSchema: StructType,
                               options: Map[String,String],
                               sparkSession: SparkSession)
  extends PartitionReader[InternalRow] {

  private var currenFile = 0
  private val outputImageType = options.getOrElse("outputImageType", "jpeg")

  def next():Boolean = {
    if (currenFile < inputPartition.files.length) {
      val file = inputPartition.files(currenFile)
      if (pageNum == file.start.toInt) {
        filename = file.filePath.toString()
        val fs = file.filePath.toPath.getFileSystem(new Configuration())
        val status = fs.getFileStatus(file.toPath)
        document = PDDocument.load(fs.open(status.getPath))
        stripper = new PDFTextStripper()
        pdfRenderer = new PDFRenderer(document)
      }
      pageNumCur = pageNum
      if (pageNum < file.length + file.start - 1 && pageNum < document.getNumberOfPages) {

        pageNum += 1
      } else {
        currenFile += 1
        if (currenFile < inputPartition.files.length) {
          pageNum = inputPartition.files(currenFile).start.toInt
        }
      }
      true
    } else {
      false
    }
  }

  private var filename: String = _
  private var pageNum: Int = inputPartition.files(currenFile).start.toInt
  private var pageNumCur: Int = 0

  private var document: PDDocument = _
  private var stripper: PDFTextStripper = _
  private var pdfRenderer: PDFRenderer = _

  override def get(): InternalRow = {
    var text = ""
    if (readDataSchema.fieldNames.contains("text")) {
      stripper.setStartPage(pageNumCur)
      stripper.setEndPage(pageNumCur)
      text = stripper.getText(document)
    }
    var image = Array[Byte]()
    if (readDataSchema.fieldNames.contains("image")) {
      val imageType = options.getOrElse("imageType", ImageType.RGB) // Default to RGB if not provided
      val resolution = options.getOrElse("resolution", "300").toInt
      val img = imageType match {
        case ImageType.BINARY =>
          pdfRenderer.renderImageWithDPI(pageNumCur, resolution, PDFBoxImageType.BINARY)
        case ImageType.GREY => pdfRenderer.renderImageWithDPI(pageNumCur, resolution, PDFBoxImageType.GRAY)
        case _ => pdfRenderer.renderImageWithDPI(pageNumCur, resolution, PDFBoxImageType.RGB)
      }
      val oStream = new ByteArrayOutputStream()
      ImageIO.write(img, outputImageType, oStream)
      image = oStream.toByteArray
      oStream.close()
    }
    InternalRow(UTF8String.fromString(filename), pageNumCur, UTF8String.fromString(text), image, inputPartition.index)
  }

  override def close(): Unit = {
    if (document != null) {
      document.close()
    }
  }
}
