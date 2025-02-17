from typing import Dict, Optional
from pyspark.sql.types import StructType, StringType, IntegerType, StructField
from pyspark.sql.utils import AnalysisException
from pyspark.sql import SparkSession
from ..schemas.image import Image
from ..schemas.document import Document


class PdfDataSource:
    """PDF DataSource for Apache Spark"""

    @staticmethod
    def short_name() -> str:
        return "pdf"

    @staticmethod
    def infer_schema(options: Dict[str, str]) -> StructType:
        """Infer the schema for PDF DataSource"""
        return StructType([
            StructField("path", StringType(), False),
            StructField("filename", StringType(), False),
            StructField("page_number", IntegerType(), False),
            StructField("partition_number", IntegerType(), False),
            StructField("text", StringType(), False),
            StructField("image", Image.column_schema(), False),
            StructField("document", Document.column_schema(), False)
        ])

    @classmethod
    def read_pdf(cls,
                 spark: SparkSession,
                 path: str,
                 image_type: str = "RGB",
                 resolution: int = 300,
                 page_per_partition: int = 5,
                 reader: str = "pdfBox",
                 ocr_config: str = "psm=3") -> "DataFrame":
        """
        Read PDF files into a Spark DataFrame

        Args:
            spark: SparkSession instance
            path: Path to PDF file(s)
            image_type: Output image type (RGB, BINARY, GREY)
            resolution: Resolution for rendering PDF page
            page_per_partition: Number of pages per partition
            reader: PDF reader to use (pdfBox or gs)
            ocr_config: Tesseract OCR configuration

        Returns:
            Spark DataFrame containing PDF data
        """
        return (spark.read.format(cls.short_name())
                .option("imageType", image_type)
                .option("resolution", str(resolution))
                .option("pagePerPartition", str(page_per_partition))
                .option("reader", reader)
                .option("ocrConfig", ocr_config)
                .load(path))