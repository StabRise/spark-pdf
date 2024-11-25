package com.stabrise.sparkpdf

import schemas.Document
import org.apache.spark.sql
import org.apache.spark.sql.{Row, SparkSession}
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
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.repl.eagerEval.enabled", "true")
      .appName(appName).master(master).getOrCreate()
  }

  test("PDFDataSource with GhostScript") {

    val pdfPath = getClass.getClassLoader.getResource("pdfs/example_image_10_page.pdf").getPath

    // Read data using PDF data source
    val pdfDF = spark.read.format("pdf")
      .option("imageType", ImageType.BINARY)
      .option("resolution", "200")
      .option("pagePerPartition", "5")
      .option("reader", PdfReader.GHOST_SCRIPT)
      .load(pdfPath)

    pdfDF.count() shouldBe 10
    pdfDF.columns should contain allOf("path", "page_number", "text", "image", "partition_number")
    pdfDF.rdd.partitions.length shouldBe 2

    pdfDF.select("path","page_number", "text", "image", "partition_number").show(23, truncate = true)

  }

  test("PDFDataSource with GhostScript and OCR") {
    val filePath = "pdfs/example_image_10_page.pdf"
    val fileName = Paths.get(filePath).getFileName.toString
    val pdfPath = getClass.getClassLoader.getResource(filePath).getPath

    // Read data using PDF data source
    val pdfDF = spark.read.format("pdf")
      .option("imageType", ImageType.BINARY)
      .option("resolution", "200")
      .option("pagePerPartition", "2")
      .option("reader", PdfReader.GHOST_SCRIPT)
      .load(pdfPath)

    pdfDF.count() shouldBe 10
    pdfDF.columns should contain allOf("path", "page_number", "text", "image", "partition_number")
    pdfDF.rdd.partitions.length shouldBe 5

    val data = pdfDF.select("document", "filename", "path").collect()

    data.head.getString(1) shouldBe fileName
    data.head.getString(2) should include (filePath)


    val document = Document(data.head.getAs[Row](0))
    document.path should include(filePath)
    document.text should include("On October 21, 2024, tech giant OpenAl announced the release")
    pdfDF.select("document.*").show(2, truncate = true)
  }


  test("PDFDataSource with PdfBox") {

    val pdfPath = getClass.getClassLoader.getResource("pdfs/example_image_10_page.pdf").getPath

    // Read data using PDF data source
    val pdfDF = spark.read.format("pdf")
      .option("imageType", ImageType.BINARY)
      .option("resolution", "200")
      .option("pagePerPartition", "2")
      .option("reader", PdfReader.PDF_BOX)
      .load(pdfPath)

    pdfDF.count() shouldBe 10
    pdfDF.columns should contain allOf("path", "page_number", "text", "image", "partition_number")
    pdfDF.rdd.partitions.length shouldBe 5

    pdfDF.select("path", "page_number", "text", "image", "partition_number").show(23, truncate = true)

  }

  test("PDFDataSource with PdfBox and OCR") {
    val filePath = "pdfs/example_image_10_page.pdf"
    val fileName = Paths.get(filePath).getFileName.toString
    val pdfPath = getClass.getClassLoader.getResource(filePath).getPath

    // Read data using PDF data source
    val pdfDF = spark.read.format("pdf")
      .option("imageType", ImageType.BINARY)
      .option("resolution", "200")
      .option("pagePerPartition", "2")
      .option("reader", PdfReader.PDF_BOX)
      .load(pdfPath)

    pdfDF.count() shouldBe 10
    pdfDF.columns should contain allOf("path", "page_number", "text", "image", "partition_number")
    pdfDF.rdd.partitions.length shouldBe 5

    val data = pdfDF.select("document", "filename", "path").collect()

    data.head.getString(1) shouldBe fileName
    data.head.getString(2) should include (filePath)

    val document = Document(data.head.getAs[Row](0))
    document.path should include (filePath)
    document.text should include ("On October 21, 2024, tech giant OpenAl announced the release")
    pdfDF.select("document.*").show(2, truncate = true)
  }

  override def afterEach(): Unit = {
    spark.stop()
  }
}

