from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, ArrayType

class Document:
    @staticmethod
    def column_schema() -> StructType:
        box_schema = StructType([
            StructField("text", StringType(), True),
            StructField("score", FloatType(), True),
            StructField("x", IntegerType(), True),
            StructField("y", IntegerType(), True),
            StructField("width", IntegerType(), True),
            StructField("height", IntegerType(), True)
        ])
        
        return StructType([
            StructField("path", StringType(), True),
            StructField("text", StringType(), True),
            StructField("outputType", StringType(), True),
            StructField("bBoxes", ArrayType(box_schema), True),
            StructField("exception", StringType(), True)
        ])