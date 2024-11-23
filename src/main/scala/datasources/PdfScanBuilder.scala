package com.stabrise.sparkpdf
package datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class PdfScanBuilder(
                           sparkSession: SparkSession,
                           fileIndex: PartitioningAwareFileIndex,
                           schema: StructType,
                           dataSchema: StructType,
                           options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {

  override def build(): PdfScan = {
    PdfScan(sparkSession,  fileIndex, schema, dataSchema, readDataSchema(), readPartitionSchema(), options)
  }
}

