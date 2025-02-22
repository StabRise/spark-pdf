package com.stabrise.sparkpdf

import schemas.Document

import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, split}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers._
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.nio.file.Paths

class PdfDatasourceSuite extends AnyFunSuite with BeforeAndAfterEach {

  private val master = "local[*]"

  private val appName = "ReadFileTest"

  var spark : SparkSession = _

  override def beforeEach(): Unit = {
    spark = new sql.SparkSession.Builder()
      .config("spark.driver.memory", "8G")
      //.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.repl.eagerEval.enabled", "true")
      .appName(appName)
      .master(master)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  test("PDFDataSource with GhostScript") {

    val (filePath, fileName, pdfDF) = readPdf(PdfReader.GHOST_SCRIPT)
    checkImegeResult(pdfDF)
  }

  private def checkImegeResult(pdfDF: DataFrame): Unit = {
    pdfDF.count() shouldBe 10
    pdfDF.columns should contain allOf("path", "page_number", "text", "image", "partition_number")
    pdfDF.rdd.partitions.length shouldBe 5
    pdfDF.select("path", "page_number", "text", "image", "partition_number").collect()
  }

  test("PDFDataSource with GhostScript and OCR") {
    val (filePath, fileName, pdfDF) = readPdf(PdfReader.GHOST_SCRIPT)
    checkOcrResulst(filePath, fileName, pdfDF)
  }

  test("PDFDataSource with PdfBox") {
    val (filePath, fileName, pdfDF) = readPdf(PdfReader.PDF_BOX)
    checkImegeResult(pdfDF)
  }

  test("PDFDataSource with empty reader") {
    val (filePath, fileName, pdfDF) = readPdf("")
    checkImegeResult(pdfDF)
  }

  test("PDFDataSource with PdfBox and OCR") {
    val (filePath, fileName, pdfDF) = readPdf(PdfReader.PDF_BOX)
    checkOcrResulst(filePath, fileName, pdfDF)
  }

  private def checkOcrResulst(filePath: String, fileName: String, pdfDF: DataFrame): Unit = {
    pdfDF.count() shouldBe 10
    pdfDF.columns should contain allOf("path", "page_number", "text", "image", "partition_number")
    pdfDF.rdd.partitions.length shouldBe 5

    val data = pdfDF.select("document", "filename", "path").collect()

    data.head.getString(1) shouldBe fileName
    data.head.getString(2) should include(filePath)

    val document = Document(data.head.getAs[Row](0))
    document.path should include(filePath)
    document.text should include("On October 21, 2024, tech giant OpenAl announced the release")
    document.bBoxes.length shouldBe 185
    //pdfDF.select("document.*").show(2, truncate = true)
  }

  private def readPdf(reader: String, filePath: String = "pdfs/example_image_10_page.pdf") = {
    val fileName = Paths.get(filePath).getFileName.toString
    val pdfPath = getClass.getClassLoader.getResource(filePath).getPath

    // Read data using PDF data source
    val pdfDF = spark.read.format("pdf")
      .option("imageType", ImageType.BINARY)
      .option("resolution", "200")
      .option("pagePerPartition", "2")
      .option("reader", reader)
      .option("ocrConfig", "psm=11")
      .load(pdfPath)
      .cache()
    (filePath, fileName, pdfDF)
  }

  override def afterEach(): Unit = {
    spark.stop()
  }
}

