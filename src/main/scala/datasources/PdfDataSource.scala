package com.stabrise.sparkpdf
package datasources

import schemas.{Document, Image}
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.binaryfile.BinaryFileFormat
import org.apache.spark.sql.types.{BinaryType, DataType, IntegerType, StringType, StructField, StructType}


class PdfDataSource extends FileDataSourceV2 with DataSourceRegister{
  override def shortName() = "pdf"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    // Infer schema logic goes here
    StructType(Seq(
      StructField("path", StringType, nullable = false),
      StructField("filename", StringType, nullable = false),
      StructField("page_number", IntegerType, nullable = false),
      StructField("partition_number", IntegerType, nullable = false),
      StructField("text", StringType, nullable = false),
      StructField("image", Image.columnSchema, nullable = false),
      StructField("document", Document.columnSchema, nullable = false)
    ))
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    PdfBatchTable(tableName, sparkSession, optionsWithoutPaths, paths, Some(schema), fallbackFileFormat)
  }

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[BinaryFileFormat]

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    PdfBatchTable(tableName, sparkSession, optionsWithoutPaths, paths, None, fallbackFileFormat)
  }
}



