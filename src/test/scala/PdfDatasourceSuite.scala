package com.stabrise.sparkpdf

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, split}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

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

  override def afterEach(): Unit = {
    spark.stop()
  }
}

