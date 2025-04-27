package com.stabrise.sparkpdf
package datasources

import org.apache.hadoop.conf.Configuration
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.pdfbox.rendering.{PDFRenderer, ImageType => PDFBoxImageType}
import org.apache.pdfbox.text.PDFTextStripper
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types.StructType
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.util.SerializableConfiguration

import java.io.ByteArrayOutputStream
import javax.imageio.ImageIO


// TODO: Need to refactor it for reduce to use state variables and make it more transparent
class PdfPartitionReaderPDFBox(inputPartition: FilePartition,
                               readDataSchema: StructType,
                               broadcastedConf: Broadcast[SerializableConfiguration],
                               options: Map[String,String])
  extends PdfPartitionReadedBase(inputPartition, readDataSchema, broadcastedConf, options) {

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
        val fs = hdfsPath.getFileSystem(broadcastedConf.value.value)
        val status = fs.getFileStatus(hdfsPath)
        document = PDDocument.load(fs.open(status.getPath), options.getOrElse("password", ""))
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
}
