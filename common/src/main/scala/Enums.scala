package com.stabrise.sparkpdf

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
}
