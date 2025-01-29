package com.stabrise.sparkpdf
package datasources

import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.PartitionedFileUtil
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.FileStatusWithMetadata

object PdfPartitionedFileUtil {

  def getHdfsPath(file: PartitionedFile):Path = {
    file.filePath.toPath
  }
  def splitFiles(
                  sparkSession: SparkSession,
                  file: FileStatusWithMetadata,
                  filePath: Path,
                  isSplitable: Boolean,
                  maxSplitBytes: Long,
                  partitionValues: InternalRow): Seq[PartitionedFile] = {
    val path = filePath
    val fs = path.getFileSystem(sparkSession.sessionState.newHadoopConf())

    // Load the PDF document
    val document = PDDocument.load(fs.open(file.getPath))
    val page_num = document.getNumberOfPages
    document.close()

    (0L until page_num by maxSplitBytes).map { offset =>
      val remaining = page_num - offset
      val size = if (remaining > maxSplitBytes) maxSplitBytes else remaining
      //TODO: temporary changed for avoid the error on Databricks, need investigate more
      //val hosts = PdfBasePartitionedFileUtil.getBlockHosts(getBlockLocations(file.fileStatus), offset, size)
      val hosts = PdfBasePartitionedFileUtil.getBlockHosts(Array.empty[BlockLocation], offset, size)
      PartitionedFile(
        partitionValues=partitionValues,
        filePath=SparkPath.fromPath(file.getPath),
        start=offset,
        length=size,
        locations=hosts,
        modificationTime=file.getModificationTime,
        fileSize=page_num.toLong)
    }
  }

  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }
}
