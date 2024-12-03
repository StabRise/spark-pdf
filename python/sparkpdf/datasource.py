from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import *


class PdfDataSource(DataSource):

    @classmethod
    def name(cls):
        return "pdf"

    def schema(self):
        return StructType([
            StructField("path", StringType()),
            StructField("image", BinaryType())
        ])

    def reader(self, schema: StructType):
        return PdfDataSourceReader(schema, self.options)


class PdfDataSourceReader(DataSourceReader):

    def __init__(self, schema, options):
        self.schema: StructType = schema
        self.options = options

    def partitions(self):
        return [InputPartition(1)]

    def read(self, partition):
        reader = self.options.get("reader", "myPyPDF")
        for _ in range(10):
            yield tuple(["filepath", bytes()])
