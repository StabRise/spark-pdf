<p align="center">
  <br/>
    <img alt="Spark Pdf" src="https://stabrise.com/media/filer_public_thumbnails/filer_public/16/d6/16d6a0d6-f162-42ad-a5a3-7dc20361ad24/sparkpdf.png__1000x300_subsampling-2.webp" width="450" style="max-width: 100%;">
  <br/>
</p>

<p align="center">
    <a target="_blank" href="https://colab.research.google.com/github/StabRise/spark-pdf/blob/main/examples/PdfDataSource.ipynb">
      <img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab Qick Start"/>
    </a>
    <a href="https://github.com/StabRise/spark-pdf/actions/">
        <img alt="Test" src="https://github.com/StabRise/spark-pdf/actions/workflows/scala.yml/badge.svg">
    </a>
    <a href="https://search.maven.org/artifact/com.stabrise/spark-pdf-spark35_2.12">
        <img alt="Maven Central Version" src="https://img.shields.io/maven-central/v/com.stabrise/spark-pdf-spark35_2.12">
    </a>
    <a href="https://github.com/StabRise/spark-pdf/blob/master/LICENSE" >
        <img src="https://img.shields.io/badge/License-AGPL%203-blue.svg" alt="License"/>
    </a>
    <a href="https://app.codacy.com/gh/StabRise/spark-pdf/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade" target="_blank">
        <img src="https://app.codacy.com/project/badge/Grade/2fde782d0c754df1b60b389799f46f0f" alt="Codacy Badge">
    </a>
</p>


<p align="center">
    <a href="https://x.com/intent/tweet?text=Check%20out%20this%20project%20on%20GitHub:%20https://github.com/StabRise/spark-pdf%20%23OpenIDConnect%20%23Security%20%23Authentication" target="_blank">
        <img src="https://img.shields.io/badge/share-000000?logo=x&logoColor=white" alt="Share on X">
    </a>
    <a href="https://www.linkedin.com/sharing/share-offsite/?url=https://github.com/StabRise/spark-pdf" target="_blank">
        <img src="https://img.shields.io/badge/share-0A66C2?logo=linkedin&logoColor=white" alt="Share on LinkedIn">
    </a>
    <a href="https://www.reddit.com/submit?title=Check%20out%20this%20project%20on%20GitHub:%20https://github.com/StabRise/spark-pdf" target="_blank">
        <img src="https://img.shields.io/badge/share-FF4500?logo=reddit&logoColor=white" alt="Share on Reddit">
    </a>
</p>

---

⭐ Star us on GitHub — it motivates us a lot!

**Source Code**: [https://github.com/StabRise/spark-pdf](https://github.com/StabRise/spark-pdf)

**Quick Start Jupyter Notebook Spark 3.5.x on Databricks**: [PdfDataSourceSpark4.ipynb](https://github.com/StabRise/spark-pdf/blob/main/examples/PdfDataSourceDatabricks.ipynb)

**Quick Start Jupyter Notebook Spark 3.x.x**: [PdfDataSource.ipynb](https://github.com/StabRise/spark-pdf/blob/main/examples/PdfDataSource.ipynb)

**Quick Start Jupyter Notebook Spark 4.0.x**: [PdfDataSourceSpark4.ipynb](https://github.com/StabRise/spark-pdf/blob/main/examples/PdfDataSourceSpark4.ipynb)

---

## Welcome to the Spark PDF

The project provides a custom data source for the [Apache Spark](https://spark.apache.org/) that allows you to read PDF files into the Spark DataFrame.

If you found useful this project, please give a star to the repository.

👉 Works on Databricks now. See the [Databricks example](https://github.com/StabRise/spark-pdf/blob/main/examples/PdfDataSourceDatabricks.ipynb).

## Key features:

- Read PDF documents to the Spark DataFrame
- Support efficient read PDF files lazy per page
- Support big files, up to 10k pages
- Support scanned PDF files (call OCR for text recognition from the images)
- No need to install Tesseract OCR, it's included in the package
- 👉 Compatible with [ScaleDP](https://github.com/StabRise/ScaleDP), an Open-Source Library for Processing Documents using AI/ML in Apache Spark.


## Requirements

- Java 8, 11, 17
- Apache Spark 3.3.2, 3.4.1, 3.5.0, 4.0.0
- Ghostscript 9.50 or later (only for the GhostScript reader)

Spark 4.0.0 is supported in the version `0.1.11` and later (need Java 17 and Scala 2.13).

## Installation

Binary package is available in the Maven Central Repository.


- **Spark 3.5.***: com.stabrise:spark-pdf-spark35_2.12:0.1.14
- **Spark 3.4.***: com.stabrise:spark-pdf-spark34_2.12:0.1.11
- **Spark 3.3.***: com.stabrise:spark-pdf-spark33_2.12:0.1.11
- **Spark 4.0.***: com.stabrise:spark-pdf-spark34_2.13:0.1.11

## Options for the data source:

- `imageType`: Oputput image type. Can be: "BINARY", "GREY", "RGB". Default: "RGB".
- `resolution`: Resolution for rendering PDF page to the image. Default: "300" dpi.
- `pagePerPartition`: Number pages per partition in Spark DataFrame. Default: "5".
- `reader`: Supports: `pdfBox` - based on PdfBox java lib, `gs` - based on GhostScript (need installation GhostScipt to the system)
- `ocrConfig`: Tesseract OCR configuration. Default: "psm=3". For more information see [Tesseract OCR Params](TesseractParams.md)

## Output Columns in the DataFrame:

The DataFrame contains the following columns:

- `path`: path to the file
- `page_number`: page number of the document
- `text`: extracted text from the text layer of the PDF page
- `image`: image representation of the page
- `document`: the OCR-extracted text from the rendered image (calls Tesseract OCR)
- `partition_number`: partition number

Output Schema:

```agsl
root
 |-- path: string (nullable = true)
 |-- filename: string (nullable = true)
 |-- page_number: integer (nullable = true)
 |-- partition_number: integer (nullable = true)
 |-- text: string (nullable = true)
 |-- image: struct (nullable = true)
 |    |-- path: string (nullable = true)
 |    |-- resolution: integer (nullable = true)
 |    |-- data: binary (nullable = true)
 |    |-- imageType: string (nullable = true)
 |    |-- exception: string (nullable = true)
 |    |-- height: integer (nullable = true)
 |    |-- width: integer (nullable = true)
 |-- document: struct (nullable = true)
 |    |-- path: string (nullable = true)
 |    |-- text: string (nullable = true)
 |    |-- outputType: string (nullable = true)
 |    |-- bBoxes: array (nullable = true)
 |    |    |-- element: struct (containsNull = true)
 |    |    |    |-- text: string (nullable = true)
 |    |    |    |-- score: float (nullable = true)
 |    |    |    |-- x: integer (nullable = true)
 |    |    |    |-- y: integer (nullable = true)
 |    |    |    |-- width: integer (nullable = true)
 |    |    |    |-- height: integer (nullable = true)
 |    |-- exception: string (nullable = true)
```
## Example of usage

### Scala

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Spark PDF Example")
  .master("local[*]")
  .config("spark.jars.packages", "com.stabrise:spark-pdf-spark35_2.12:0.1.11")
  .getOrCreate()
  
val df = spark.read.format("pdf")
  .option("imageType", "BINARY")
  .option("resolution", "200")
  .option("pagePerPartition", "2")
  .option("reader", "pdfBox")
  .option("ocrConfig", "psm=11")
  .load("path to the pdf file(s)")

df.select("path", "document").show()
```

### Python

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("SparkPdf") \
    .config("spark.jars.packages", "com.stabrise:spark-pdf-spark35_2.12:0.1.11") \
    .getOrCreate()

df = spark.read.format("pdf") \
    .option("imageType", "BINARY") \
    .option("resolution", "200") \
    .option("pagePerPartition", "2") \
    .option("reader", "pdfBox") \
    .option("ocrConfig", "psm=11") \
    .load("path to the pdf file(s)")

df.select("path", "document").show()
```

## Disclaimer

This project is not affiliated with, endorsed by, or connected to the Apache Software Foundation or Apache Spark.
