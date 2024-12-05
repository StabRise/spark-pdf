package com.stabrise.sparkpdf
package datasources

import org.apache.hadoop.fs.BlockLocation

object PdfBasePartitionedFileUtil{
  // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
  // pair that represents a segment of the same file, find out the block that contains the largest
  // fraction the segment, and returns location hosts of that block. If no such block can be found,
  // returns an empty array.
  def getBlockHosts(
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
