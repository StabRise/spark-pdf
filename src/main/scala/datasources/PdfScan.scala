package com.stabrise.sparkpdf
package datasources

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import java.util.Locale
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

case class PdfScan(
                    sparkSession: SparkSession,
                    fileIndex: PartitioningAwareFileIndex,
                    schema: StructType,
                    dataSchema: StructType,
                    readDataSchema: StructType,
                    readPartitionSchema: StructType,
                    options: CaseInsensitiveStringMap,
                    partitionFilters: Seq[Expression] = Seq.empty,
                    dataFilters: Seq[Expression] = Seq.empty
                  ) extends Scan with Batch{
  override def readSchema(): StructType =  schema

  override def toBatch: Batch = this


  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis


  private def normalizeName(name: String): String = {
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
  }

  override def planInputPartitions(): Array[InputPartition] = {
    partitions.toArray
  }

  protected lazy val partitions: Seq[FilePartition] = {

    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    val maxSplitBytes = options.getOrDefault("pagePerPartition", DefaultOptions.PAGE_PER_PARTITION).toInt

    def toAttributeField(field: StructField): AttributeReference =
      AttributeReference(field.name, field.dataType, field.nullable, field.metadata)()

    def toAttributes(schema: StructType): Seq[AttributeReference] = schema.map(field => toAttributeField(field))



    val partitionAttributes = toAttributes(fileIndex.partitionSchema)
    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
    val readPartitionAttributes = readPartitionSchema.map { readField =>
      attributeMap.getOrElse(normalizeName(readField.name),
        throw new Exception("cannotFindPartitionColumnInPartitionSchemaError"))
    }
    lazy val partitionValueProject =
      GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
    val splitFiles = selectedPartitions.flatMap { partition =>
      // Prune partition values if part of the partition columns are not required.
      val partitionValues = if (readPartitionAttributes != partitionAttributes) {
        partitionValueProject(partition.values).copy()
      } else {
        partition.values
      }
      partition.files.flatMap { file =>
        val filePath = file.getPath
        PdfPartitionedFileUtil.splitFiles(
          sparkSession = sparkSession,
          file = file,
          filePath = filePath,
          isSplitable = true,
          maxSplitBytes = maxSplitBytes,
          partitionValues = partitionValues
        )
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }

    def getFilePartitions(
                           sparkSession: SparkSession,
                           partitionedFiles: Seq[PartitionedFile],
                           maxSplitBytes: Long): Seq[FilePartition] = {
      val partitions = new ArrayBuffer[FilePartition]
      val currentFiles = new ArrayBuffer[PartitionedFile]
      var currentSize = 0L

      /** Close the current partition and move to the next. */
      def closePartition(): Unit = {
        if (currentFiles.nonEmpty) {
          // Copy to a new Array.
          val newPartition = FilePartition(partitions.size, currentFiles.toArray)
          partitions += newPartition
        }
        currentFiles.clear()
        currentSize = 0
      }

      val openCostInBytes = sparkSession.sessionState.conf.filesOpenCostInBytes
      // Assign files to partitions using "Next Fit Decreasing"
      partitionedFiles.foreach { file =>
        if (currentSize + file.length > maxSplitBytes) {
          closePartition()
        }
        // Add the given file to the current partition.
        currentSize += file.length + openCostInBytes
        currentFiles += file
      }
      closePartition()
      partitions.toSeq
    }

    getFilePartitions(sparkSession, splitFiles, maxSplitBytes)
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    new PdfPartitionReaderFactory(readDataSchema,
    options.asScala.toMap,
      broadcastedConf)}
}

