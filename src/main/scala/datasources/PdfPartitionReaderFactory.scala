package com.stabrise.sparkpdf
package datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types.StructType

class PdfPartitionReaderFactory(readDataSchema: StructType,
                                options: Map[String,String],
                                sparkSession: SparkSession) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    options.get("reader") match {
      case Some(PdfReader.PDF_BOX) => new PdfPartitionReaderPDFBox(partition.asInstanceOf[FilePartition], readDataSchema, options, sparkSession)
      case Some(PdfReader.GHOST_SCRIPT) => new PdfPartitionReaderGS(partition.asInstanceOf[FilePartition], readDataSchema, options)
      case _ => throw new RuntimeException(s"Unsupported reader for type: ${options.get("reader")}")
    }
  }
}
