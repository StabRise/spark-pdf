package com.stabrise.sparkpdf
package datasources

import org.apache.hadoop.conf.Configuration
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.rendering.{PDFRenderer, ImageType => PDFBoxImageType}
import org.apache.pdfbox.text.PDFTextStripper
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types.StructType

import java.io.ByteArrayOutputStream
import javax.imageio.ImageIO


// TODO: Need to refactor it for reduce to use state variables and make it more transparent
class PdfPartitionReaderPDFBox(inputPartition: FilePartition,
                               readDataSchema: StructType,
                               options: Map[String,String])
  extends PdfPartitionReadedBase(inputPartition, readDataSchema, options) {

  private var currenFile = 0
  private val outputImageType = options.getOrElse("outputImageType", DefaultOptions.OUTPUT_IMAGE_TYPE)
  private var pageNum: Int = inputPartition.files(currenFile).start.toInt
  private var document: PDDocument = _
  private var stripper: PDFTextStripper = _
  private var pdfRenderer: PDFRenderer = _

  def next():Boolean = {
    if (currenFile < inputPartition.files.length) {
      val file = inputPartition.files(currenFile)
      if (pageNum == file.start.toInt) {
        filename = file.filePath.toString()
        val hdfsPath = PdfPartitionedFileUtil.getHdfsPath(file)
        val fs = hdfsPath.getFileSystem(new Configuration())
        val status = fs.getFileStatus(hdfsPath)
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

  override def getSearchableText(): String = {
    stripper.setStartPage (pageNumCur + 1)
    stripper.setEndPage (pageNumCur + 1)
    stripper.getText (document)
  }

  override def renderImage(resolution: Int): Array[Byte] = {
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
  }

  override def close(): Unit = {
    if (document != null) {
      document.close()
    }
    tesseract.close()
  }

  private def hasTextLayer: Boolean = {
    stripper.setStartPage(pageNumCur + 1)
    stripper.setEndPage(pageNumCur + 1)
    val text = stripper.getText(document)
    text.trim.nonEmpty
  }

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
      if (forceOCR || text.isEmpty || !hasTextLayer) {
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
