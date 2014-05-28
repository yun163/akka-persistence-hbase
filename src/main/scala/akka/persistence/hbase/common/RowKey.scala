package akka.persistence.hbase.common

import akka.persistence.hbase.journal.PluginPersistenceSettings
import akka.persistence.hbase.common.Const._
import org.apache.hadoop.hbase.util.Bytes

case class RowKey(processorId: String, sequenceNr: Long, saltId: Option[Long] = None)(implicit settings: PluginPersistenceSettings) {

  def toBytes = Bytes.toBytes(toKeyString)

  protected def getSalt = saltId match {
    case Some(salt) => RowKey.padNum(salt, ROW_KEY_PARTITION_SALT_LEN)
    case None => RowKey.padNum(partition(sequenceNr), ROW_KEY_PARTITION_SALT_LEN)
  }

  def toKeyString = getSalt + RowKey.padId(processorId, ROW_KEY_PRSOR_ID_LEN_MAX) + RowKey.padNum(sequenceNr, ROW_KEY_SEQ_NUM_LEN)

  /** Used to avoid writing all data to the same region - see "hot region" problem */
  private def partition(sequenceNr: Long): Long = (sequenceNr - 1) % settings.partitionCount
}

object SaltedRowKey {
  def apply(processorId: String, sequenceNr: Long, salt: Long)(implicit settings: PluginPersistenceSettings): RowKey = {
    RowKey(processorId, sequenceNr, Some(salt))
  }
}

object RowKey {

  def padId(processorId: String, howLong: Int) = {
    if (processorId.length < howLong) {
      processorId.padTo(howLong, "0").mkString.substring(0, howLong)
    } else if (processorId.length > howLong) {
      ShortStringGenerator.genMd5String(processorId).substring(0, howLong)
    } else {
      processorId
    }
  }

  def padNum(l: Long, howLong: Int) = String.valueOf(l).reverse.padTo(howLong, "0").reverse.mkString.substring(0, howLong)

  /**
   * Since we're salting (prefixing) the entries with partition numbers,
   * we must use this pattern for scanning for "all messages for processorX"
   */
  def patternForProcessor(processorId: String)(implicit journalConfig: PluginPersistenceSettings) =
    s"""[0-9]{$ROW_KEY_PARTITION_SALT_LEN}${padId(processorId, ROW_KEY_PRSOR_ID_LEN_MAX)}[0-9]{$ROW_KEY_SEQ_NUM_LEN}"""

  /** First key possible, similar to: `00id000000000000000000000` */
  def firstKey(processorId: String)(implicit journalConfig: PluginPersistenceSettings) =
    SaltedRowKey(processorId, 0, 0)

  /** First key possible, similar to: `00id000000000000000000000` */
  def saltedFirstKey(processorId: String, salt: Long)(implicit journalConfig: PluginPersistenceSettings) =
    SaltedRowKey(processorId, 0, salt)

  /** Scan end key, similar to: `xxxidtoSequenceNr` */
  def saltedToKey(processorId: String, toSequenceNr: Long, salt: Long)(implicit journalConfig: PluginPersistenceSettings) = {
    // seqNum + 1 to make hbase query include the last key, because asyncHbse client's endKey is excluded
    val toSeqNr: Long = toSequenceNr match {
      case num if num < Long.MaxValue => num + 1
      case _ => Long.MaxValue
    }
    SaltedRowKey(processorId, toSeqNr, salt)
  }

  /** Scan end key, similar to: `xxxidtoSequenceNr` */
  def toKey(processorId: String, toSequenceNr: Long)(implicit journalConfig: PluginPersistenceSettings) = {
    // seqNum + 1 to make hbase query include the last key, because asyncHbse client's endKey is excluded
    val toSeqNr: Long = toSequenceNr match {
      case num if num < Long.MaxValue => num + 1
      case _ => Long.MaxValue
    }
    SaltedRowKey(processorId, toSeqNr, 0)
  }

  def toSequenceNr(sequenceNr: Long) = {
    sequenceNr match {
      case num if num < Long.MaxValue => num + 1
      case _ => Long.MaxValue
    }
  }

  /** Last key possible, similar to: `xxxidLong.MaxValue` */
  def saltedLastKey(processorId: String, salt: Long)(implicit journalConfig: PluginPersistenceSettings) =
    SaltedRowKey(processorId, Long.MaxValue, salt)

  /** Last key possible, similar to: `000idLong.MaxValue` */
  def lastKey(processorId: String)(implicit journalConfig: PluginPersistenceSettings) =
    SaltedRowKey(processorId, Long.MaxValue, 0)
}