package com.stabrise.sparkpdf
package ocr

import org.bytedeco.leptonica.PIX
import org.bytedeco.leptonica.global.leptonica._
import org.bytedeco.tesseract.TessBaseAPI
import net.sourceforge.tess4j.util.LoadLibs


class TesseractBytedeco(val lang: String="eng", config: String) {

  private val conf = config.split(",").map { pair =>
    val Array(key, value) = pair.split("=")
    key -> value
  }.toMap

  private val api = new TessBaseAPI()

  private val dataPath = LoadLibs.extractTessResources("tessdata").getAbsolutePath

  def imageToText(bi: Array[Byte]): String = {
    api.Init(dataPath, lang)

    if (conf.contains("psm")) {
      api.SetPageSegMode(conf("psm").toInt)
    }
    conf.foreach { case (key, value) =>
      api.SetVariable(key, value)
    }
    setImage(bi)
    val text = api.GetUTF8Text().getString
    api.Clear()
    text
  }

  def setImage(bi: Array[Byte]): Unit = {
    val pix: PIX = pixReadMem(bi, bi.length)
    api.SetImage(pix
    )
  }


  def close(): Unit = {
    api.End()
  }
}
