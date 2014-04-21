package akka.persistence.hbase.common

import akka.persistence.hbase.journal.PluginPersistenceSettings
import akka.persistence.hbase.common.Const._
import org.apache.hadoop.hbase.util.Bytes

case class RowKey(processorId: String, sequenceNr: Long)(implicit hBasePersistenceSettings: PluginPersistenceSettings) {

  def part = partition(sequenceNr)
  val toBytes = Bytes.toBytes(toKeyString)

  def toKeyString =
    s"${RowKey.padNum(part, ROW_KEY_PARTITION_SALT_LEN)}~${RowKey.padId(processorId, ROW_KEY_PRSOR_ID_LEN_MAX)}~${RowKey.padNum(sequenceNr, ROW_KEY_SEQ_NUM_LEN)}"

  /** Used to avoid writing all data to the same region - see "hot region" problem */
  private def partition(sequenceNr: Long): Long =
    sequenceNr % hBasePersistenceSettings.partitionCount
}

object RowKey {

  def padId(processorId: String, howLong: Int) = processorId.padTo(howLong, "0").mkString.substring(0, howLong)

  def padNum(l: Long, howLong: Int) = String.valueOf(l).reverse.padTo(howLong, "0").reverse.mkString.substring(0, howLong)

  /**
   * Since we're salting (prefixing) the entries with partition numbers,
   * we must use this pattern for scanning for "all messages for processorX"
   */
  def patternForProcessor(processorId: String)(implicit journalConfig: PluginPersistenceSettings) =
    s"""[0-9]{$ROW_KEY_PARTITION_SALT_LEN}~${padId(processorId, ROW_KEY_PRSOR_ID_LEN_MAX)}~[0-9]{$ROW_KEY_SEQ_NUM_LEN}"""

  /** First key possible, similar to: `0~id~000000000000000000000` */
  def firstForProcessor(processorId: String)(implicit journalConfig: PluginPersistenceSettings) =
    RowKey(processorId, 0)

  /** Scan end key, similar to: `999~id~toSequenceNr` */
  def toKeyForProcessor(processorId: String, toSequenceNr: Long)(implicit journalConfig: PluginPersistenceSettings) = {
    // seqNum + 1 to make hbase query include the last key, because hbse client's endKey is excluded
    val toSeqNr: Long = toSequenceNr match {
      case num if num < Long.MaxValue => num + 1
      case _ => Long.MaxValue
    }
    Bytes.toBytes(s"""${"9" * ROW_KEY_PARTITION_SALT_LEN}~${padId(processorId, ROW_KEY_PRSOR_ID_LEN_MAX)}~${padNum(toSeqNr, ROW_KEY_SEQ_NUM_LEN)}""")
  }

  /** Last key possible, similar to: `999~id~Long.MaxValue` */
  def lastForProcessor(processorId: String)(implicit journalConfig: PluginPersistenceSettings) =
    Bytes.toBytes(s"""${"9" * ROW_KEY_PARTITION_SALT_LEN}~${padId(processorId, ROW_KEY_PRSOR_ID_LEN_MAX)}~${padNum(Long.MaxValue, ROW_KEY_SEQ_NUM_LEN)}""")

}