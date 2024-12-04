package com.stabrise.sparkpdf
package datasources

import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}
import org.apache.pdfbox.pdmodel.PDDocument
import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.FileStatusWithMetadata

object PdfPartitionedFileUtil {
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
      val hosts = getBlockHosts(getBlockLocations(file), offset, size)
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

  private def getBlockLocations(file: FileStatusWithMetadata): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }
  // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
  // pair that represents a segment of the same file, find out the block that contains the largest
  // fraction the segment, and returns location hosts of that block. If no such block can be found,
  // returns an empty array.
  private def getBlockHosts(
                             blockLocations: Array[BlockLocation],
                             offset: Long,
                             length: Long): Array[String] = {
    val candidates = blockLocations.map {
      // The fragment starts from a position within this block. It handles the case where the
      // fragment is fully contained in the block.
      case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
        b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

      // The fragment ends at a position within this block
      case b if b.getOffset < offset + length && offset + length < b.getOffset + b.getLength =>
        b.getHosts -> (offset + length - b.getOffset)

      // The fragment fully contains this block
      case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
        b.getHosts -> b.getLength

      // The fragment doesn't intersect with this block
      case b =>
        b.getHosts -> 0L
    }.filter { case (hosts, size) =>
      size > 0L
    }

    if (candidates.isEmpty) {
      Array.empty[String]
    } else {
      val (hosts, _) = candidates.maxBy { case (_, size) => size }
      hosts
    }
  }
}
