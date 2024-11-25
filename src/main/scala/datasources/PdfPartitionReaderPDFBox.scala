package com.stabrise.sparkpdf
package datasources

import schemas.Box
import ocr.TesseractBytedeco
import org.apache.hadoop.conf.Configuration
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.rendering.{PDFRenderer, ImageType => PDFBoxImageType}
import org.apache.pdfbox.text.PDFTextStripper
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.nio.file.Paths
import java.io.ByteArrayOutputStream
import javax.imageio.ImageIO


// TODO: Need to refactor it for reduce to use state variables and make it more transparent
class PdfPartitionReaderPDFBox(inputPartition: FilePartition,
                               readDataSchema: StructType,
                               options: Map[String,String],
                               sparkSession: SparkSession)
  extends PartitionReader[InternalRow] {

  private var currenFile = 0
  private val outputImageType = options.getOrElse("outputImageType", DefaultOptions.OUTPUT_IMAGE_TYPE)
  private lazy val tesseract = new TesseractBytedeco()
  private var filename: String = _
  private var pageNum: Int = inputPartition.files(currenFile).start.toInt
  private var pageNumCur: Int = 0
  private var document: PDDocument = _
  private var stripper: PDFTextStripper = _
  private var pdfRenderer: PDFRenderer = _

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

  override def get(): InternalRow = {
    val resolution = options.getOrElse("resolution", DefaultOptions.RESOLUTION).toInt
    val text = if (readDataSchema.fieldNames.contains("text")) {
      stripper.setStartPage(pageNumCur + 1)
      stripper.setEndPage(pageNumCur + 1)
      stripper.getText(document)
    } else ""

    // Render the image from the PDF
    val image = if (readDataSchema.fieldNames.contains("image") || readDataSchema.fieldNames.contains("document")) {
      val imageType = options.getOrElse("imageType", DefaultOptions.IMAGE_TYPE) // Default to RGB if not provided
      val img = imageType match {
        case ImageType.BINARY =>
          pdfRenderer.renderImageWithDPI(pageNumCur, resolution, PDFBoxImageType.BINARY)
        case ImageType.GREY => pdfRenderer.renderImageWithDPI(pageNumCur, resolution, PDFBoxImageType.GRAY)
        case _ => pdfRenderer.renderImageWithDPI(pageNumCur, resolution, PDFBoxImageType.RGB)
      }
      val oStream = new ByteArrayOutputStream()
      ImageIO.write(img, outputImageType, oStream)
      val image = oStream.toByteArray
      oStream.close()
      image
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

  override def close(): Unit = {
    if (document != null) {
      document.close()
    }
    tesseract.close()
  }
}
