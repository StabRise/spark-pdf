package com.stabrise.sparkpdf
package ocr

import com.stabrise.sparkpdf.schemas.{Box, BoxWithMeta, Document}
import com.sun.jna.Native.getDefaultStringEncoding
import org.bytedeco.leptonica.PIX
import org.bytedeco.leptonica.global.leptonica._
import org.bytedeco.tesseract.{ResultIterator, TessBaseAPI}
import net.sourceforge.tess4j.util.LoadLibs
import org.bytedeco.javacpp.IntPointer
import org.bytedeco.tesseract.global.tesseract._


class TesseractBytedeco(val lang: String="eng", config: String) {

  private val conf = config.split(",").map { pair =>
    val Array(key, value) = pair.split("=")
    key -> value
  }.toMap

  private val api = new TessBaseAPI()

  private val dataPath = LoadLibs.extractTessResources("tessdata").getAbsolutePath

  def imageToText(bi: Array[Byte]): String = {
    initOcr(bi)
    val text = api.GetUTF8Text().getString
    api.Clear()
    text
  }

  private def initOcr(bi: Array[Byte]): Unit = {
    api.Init(dataPath, lang)

    if (conf.contains("psm")) {
      api.SetPageSegMode(conf("psm").toInt)
    }
    conf.foreach { case (key, value) =>
      api.SetVariable(key, value)
    }
    setImage(bi)
  }

  def imageToDocument(bi: Array[Byte], level: Int, threshold: Int, path: String): Document = {
    initOcr(bi)
    val text = api.GetUTF8Text().getString
    val boxes = new Iterator(api.GetIterator, level)
      .toList
      .filter(res => res.text != null)
      .filter(box => box.score > threshold)

    api.Clear()
    api.End()

    Document(path, text, "ocr", boxes.map(_.toBox()).toArray)
  }

  def setImage(bi: Array[Byte]): Unit = {
    val pix: PIX = pixReadMem(bi, bi.length)
    api.SetImage(pix
    )
  }

  def close(): Unit = {
    api.End()
  }

  private def getBox(iter: ResultIterator, level: Int=PIL.WORD): BoxWithMeta = {
    val x1 = new IntPointer(1l)
    val y1 = new IntPointer(1l)
    val x2 = new IntPointer(1l)
    val y2 = new IntPointer(1l)
    iter.BoundingBox(level, x1, y1, x2, y2)
    val symbolPointer = iter.GetUTF8Text(level)
    val encoding = getDefaultStringEncoding
    val text = Option(symbolPointer).map(_.getString(encoding)).getOrElse("")
    val confidence = iter.Confidence(level)
    TessDeleteText(symbolPointer)
    val isStartWord = iter.IsAtBeginningOf(PIL.WORD)
    val isStartLine = iter.IsAtBeginningOf(PIL.TEXTLINE)

    BoxWithMeta(
      text = text,
      score = confidence,
      x = x1.get,
      y = y1.get,
      width = x2.get - x1.get,
      height = y2.get - y1.get,
      isStartWord = isStartWord,
      isStartLine = isStartLine)
  }
  private class Iterator(iter: ResultIterator, level: Int) {
    iter.Begin()
    def toList: List[BoxWithMeta] = iter match {
      case iter:ResultIterator =>
        @annotation.tailrec
        def loop(rem: ResultIterator, acc: List[BoxWithMeta]): List[BoxWithMeta] =
          if (rem.Next(level))
            loop(rem, getBox(rem, level) :: acc)
          else
            acc.reverse
        loop(iter, List(getBox(iter, level)))
      case _ => Nil
    }
  }
}
