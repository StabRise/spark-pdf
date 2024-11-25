package com.stabrise.sparkpdf
package datasources

import ocr.TesseractBytedeco
import schemas.Box
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

import java.io.{ByteArrayOutputStream, InputStream}
import java.net.URI
import java.nio.file.Paths
import scala.annotation.tailrec
import scala.sys.process.{Process, ProcessLogger}

// TODO: Need to refactor it for reduce to use state variables and make it more transparent
class PdfPartitionReaderGS(inputPartition: FilePartition, readDataSchema: StructType, options: Map[String,String])
  extends PdfPartitionReadedBase(inputPartition, readDataSchema, options) {
  private var currentFileIndex = 0
  private var currentFile: PartitionedFile = _
  var document: InputStream = _
  private var pageNum: Int = inputPartition.files(currentFileIndex).start.toInt


  private def loadDocument(): Unit = {
    filename = new Path(currentFile.filePath.toString()).toString
    pageNum = currentFile.start.toInt
  }

  override def next(): Boolean = {
    if (currentFileIndex < inputPartition.files.length) {
      currentFile = inputPartition.files(currentFileIndex)
      if (pageNum == currentFile.start.toInt) {
        loadDocument()
      }
      pageNum += 1
      pageNumCur = pageNum
      if (pageNum >= currentFile.length + currentFile.start) {
        currentFileIndex += 1
      }
      true
    } else {
      false
    }
  }

  override def renderImage(resolution: Int): Array[Byte] = {
    val imageType = options.getOrElse("imageType", DefaultOptions.IMAGE_TYPE) // Default to RGB if not provided

    val imgParam = imageType match {
      case ImageType.BINARY => "pngmono"
      case ImageType.GREY => "pnggray"
      case ImageType.RGB => "png256"
      case _ => "png16m"
    }

    @tailrec
    def retry[T](n: Int)(fn: => T): T = {
      try {
        fn
      } catch {
        case e: Throwable =>
          if (n > 1) retry(n - 1)(fn)
          else throw e
      }
    }

    val name = new URI(filename).getPath

    def render_page(name: String) = {
      val stdout = new ByteArrayOutputStream()
      val command = Array("gs", "-q", "-dNOPAUSE", "-dBATCH", "-sDEVICE=" + imgParam, "-r" + resolution,
        "-dFirstPage=" + pageNum, "-dLastPage=" + pageNum, "-sOutputFile=-", name)
      val exitCode = Process(command) #> stdout ! ProcessLogger(line => ())

      if (exitCode != 0) {
        throw new Exception("Failed to render PDF page")
      }
      val image = stdout.toByteArray
      stdout.close()
      image
    }

    val image = retry(options.getOrElse("retry", "3").toInt) {
      render_page(name)
    }
    image
  }

  override def close(): Unit = {
    tesseract.close()
  }
}
