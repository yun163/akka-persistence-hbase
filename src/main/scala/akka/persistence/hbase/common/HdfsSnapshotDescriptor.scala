package akka.persistence.hbase.common

import akka.persistence.SnapshotMetadata
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.FileStatus

case class HdfsSnapshotDescriptor(processorId: String, seqNumber: Long, timestamp: Long) {
  def toFilename = s"snapshot~$processorId~$seqNumber~$timestamp"
}

object HdfsSnapshotDescriptor {
  def SnapshotNamePattern(processorId: String): scala.util.matching.Regex = {
    s"""snapshot~$processorId~([0-9]+)~([0-9]+)""".r
  }

  def apply(meta: SnapshotMetadata): HdfsSnapshotDescriptor =
    HdfsSnapshotDescriptor(meta.processorId, meta.sequenceNr, meta.timestamp)

  def from(status: FileStatus, processorId: String): Option[HdfsSnapshotDescriptor] = {
    val namePattern = SnapshotNamePattern(processorId)
    FilenameUtils.getBaseName(status.getPath.toString) match {
      case namePattern(seqNumber, timestamp) =>
        Some(HdfsSnapshotDescriptor(processorId, seqNumber.toLong, timestamp.toLong))

      case _ => None
    }
  }
}
