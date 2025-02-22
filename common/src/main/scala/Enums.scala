package com.stabrise.sparkpdf

import org.bytedeco.tesseract.global.tesseract._

object PdfReader {
  val PDF_BOX = "pdfBox"
  val GHOST_SCRIPT = "gs"
}

object ImageType {
  val BINARY = "BINARY"
  val RGB = "RGB"
  val GREY = "GREY"
}

object DefaultOptions {
  val RESOLUTION = "300"
  val IMAGE_TYPE = ImageType.RGB
  val PAGE_PER_PARTITION = "5"
  val OUTPUT_IMAGE_TYPE = "jpeg"
  val OCR_CONFIG = "psm=3"
  var READER = PdfReader.PDF_BOX
}

object PSM {
  val OSD_ONLY: Int = PSM_OSD_ONLY
  val AUTO_OSD: Int = PSM_AUTO_OSD
  val AUTO_ONLY: Int = PSM_AUTO_ONLY
  val AUTO: Int = PSM_AUTO
  val SINGLE_COLUMN: Int = PSM_SINGLE_COLUMN
  val SINGLE_BLOCK_VERT_TEXT: Int = PSM_SINGLE_BLOCK_VERT_TEXT
  val SINGLE_BLOCK: Int = PSM_SINGLE_BLOCK
  val SINGLE_LINE: Int = PSM_SINGLE_LINE
  val SINGLE_WORD: Int = PSM_SINGLE_WORD
  val CIRCLE_WORD: Int = PSM_CIRCLE_WORD
  val SINGLE_CHAR: Int = PSM_SINGLE_CHAR
  val SPARSE_TEXT: Int = PSM_SPARSE_TEXT
  val SPARSE_TEXT_OSD: Int = PSM_SPARSE_TEXT_OSD
}


object OEM {
  val TESSERACT_ONLY: Int = OEM_TESSERACT_ONLY
  val LSTM_ONLY: Int = OEM_LSTM_ONLY
  val TESSERACT_LSTM_COMBINED: Int = OEM_TESSERACT_LSTM_COMBINED
  val DEFAULT: Int = OEM_DEFAULT
}


object PIL {
  val BLOCK: Int = RIL_BLOCK
  val PARAGRAPH: Int = RIL_PARA
  val TEXTLINE: Int = RIL_TEXTLINE
  val WORD: Int = RIL_WORD
  val SYMBOL: Int = RIL_SYMBOL
}


