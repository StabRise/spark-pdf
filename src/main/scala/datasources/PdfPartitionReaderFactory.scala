package com.stabrise.sparkpdf
package datasources

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.FilePartition
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

class PdfPartitionReaderFactory(readDataSchema: StructType,
                                options: Map[String,String],
                                broadcastedConf: Broadcast[SerializableConfiguration]) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    options.get("reader") match {
      case Some(PdfReader.PDF_BOX) => new PdfPartitionReaderPDFBox(partition.asInstanceOf[FilePartition], readDataSchema, broadcastedConf, options)
      case Some(PdfReader.GHOST_SCRIPT) => new PdfPartitionReaderGS(partition.asInstanceOf[FilePartition], readDataSchema, broadcastedConf, options)
      case _ => new PdfPartitionReaderPDFBox(partition.asInstanceOf[FilePartition], readDataSchema, broadcastedConf, options)
    }
  }
}
