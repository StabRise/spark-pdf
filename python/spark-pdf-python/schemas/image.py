from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BinaryType

class Image:
    @staticmethod
    def column_schema() -> StructType:
        return StructType([
            StructField("path", StringType(), True),
            StructField("resolution", IntegerType(), True),
            StructField("data", BinaryType(), True),
            StructField("imageType", StringType(), True),
            StructField("exception", StringType(), True),
            StructField("height", IntegerType(), True),
            StructField("width", IntegerType(), True)
        ])