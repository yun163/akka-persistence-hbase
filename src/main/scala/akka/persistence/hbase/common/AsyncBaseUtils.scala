package akka.persistence.hbase.common

import akka.persistence.hbase.common.Columns._
import akka.persistence.hbase.journal.PluginPersistenceSettings
import akka.persistence.hbase.journal.RowTypeMarkers._
import java.{ util => ju }
import java.util.{ ArrayList => JArrayList }
import org.apache.hadoop.hbase.util.Bytes
import org.hbase.async._
import scala.concurrent.{ ExecutionContext, Future }
import scala.util.Sorting

import DeferredConversions._
import collection.JavaConversions._
import akka.event.LoggingAdapter

trait AsyncBaseUtils {

  val client: HBaseClient

  implicit val settings: PluginPersistenceSettings
  implicit val executionContext: ExecutionContext
  implicit val logger: LoggingAdapter

  def getTable: String
  def Table = Bytes.toBytes(getTable)
  def getFamily: String
  def Family = Bytes.toBytes(getFamily)

  protected def isSnapshotRow(columns: Seq[KeyValue]): Boolean =
    ju.Arrays.equals(findColumn(columns, Marker).value, SnapshotMarkerBytes)

  protected def findColumn(columns: Seq[KeyValue], qualifier: Array[Byte]): KeyValue =
    columns find { kv =>
      ju.Arrays.equals(kv.qualifier, qualifier)
    } getOrElse {
      throw new RuntimeException(s"Unable to find [${Bytes.toString(qualifier)}}] field from: ${columns.map(kv => Bytes.toString(kv.qualifier))}")
    }

  protected def deleteRow(key: Array[Byte]): Future[Unit] = {
    // log.debug(s"Permanently deleting row: ${Bytes.toString(key)}")
    executeDelete(key)
  }

  protected def markRowAsDeleted(key: Array[Byte]): Future[Unit] = {
    // log.debug(s"Marking as deleted, for row: ${Bytes.toString(key)}")
    executePut(key, Array(Marker), Array(DeletedMarkerBytes))
  }

  protected def executeDelete(key: Array[Byte]): Future[Unit] = {
    val request = new DeleteRequest(Table, key)
    client.delete(request)
  }

  protected def executePut(key: Array[Byte], qualifiers: Array[Array[Byte]], values: Array[Array[Byte]], falseFlush: Boolean = false): Future[Unit] = {
    val request = new PutRequest(Table, key, Family, qualifiers, values)
    val f = client.put(request)
    if (falseFlush) f.map(_ => flushWrites()) else f
  }

  /**
   * Sends the buffered commands to HBase. Does not guarantee that they "complete" right away.
   */
  def flushWrites() {
    client.flush()
  }

  protected def newScanner() = {
    val scanner = client.newScanner(Table)
    scanner.setFamily(Family)
    scanner
  }

  protected def newSaltedScanner(partitionCount: Int) = {
    new SaltedScanner(client, partitionCount, Table, Family)
  }
}

class SaltedScanner(client: HBaseClient, partitionCount: Int, table: Array[Byte], family: Array[Byte])(implicit val settings: PluginPersistenceSettings, implicit val executionContext: ExecutionContext, implicit val logger: LoggingAdapter) {
  val scanners: Seq[Scanner] = for (part <- 0 until partitionCount) yield { newPlainScanner() }

  private def newPlainScanner() = {
    val scanner = client.newScanner(table)
    scanner.setFamily(family)
    scanner
  }

  def setSaltedStartKeys(processorId: String, startSequenceNr: Long) {
    for (part <- 0 until partitionCount) scanners(part).setStartKey(SaltedRowKey(processorId, startSequenceNr, part).toBytes)
  }

  def setSaltedStopKeys(processorId: String, stopSequenceNr: Long) {
    for (part <- 0 until partitionCount) scanners(part).setStopKey(SaltedRowKey(processorId, stopSequenceNr, part).toBytes)
  }

  def setKeyRegexp(processorId: String) {
    for (part <- 0 until partitionCount) scanners(part).setKeyRegexp(RowKey.patternForProcessor(processorId))
  }

  def setMaxNumRows(batchSize: Int) {
    for (part <- 0 until partitionCount) scanners(part).setMaxNumRows(batchSize)
  }

  private def innerNextRows(sanner: Scanner): Future[AsyncBaseRows] = {
    sanner.nextRows()
  }

  def nextRows(nrows: Int): Future[AsyncBaseRows] = {
    setMaxNumRows(nrows)
    nextRows()
  }

  def nextRows(): Future[AsyncBaseRows] = {
    val futures =
      (0 until partitionCount) map {
        part =>
          innerNextRows(scanners(part))
      }
    Future.sequence(futures) map {
      case rowsList: Seq[AsyncBaseRows] =>
        val list = new AsyncBaseRows
        if (rowsList != null && rowsList.size > 0) {
          for (part <- 0 until partitionCount) {
            if (rowsList(part) != null && rowsList(part).size() > 0) {
              list.addAll(rowsList(part))
            }
          }
        }
        if (list.isEmpty) {
          null
        } else {
          val rows: Seq[RowWrapper] = list map { row => RowWrapper(row) }
          val rows1 = rows.toArray
          Sorting.quickSort(rows1)
          val arr = new AsyncBaseRows
          rows1.foreach(wrapper => arr.add(wrapper.row))
          //          renderList(arr)
          arr
        }
      case _ => null
    }
  }

  def close() {
    scanners map { _.close() }
  }

  private def renderList(rows: AsyncBaseRows) {
    println("#" * 50)
    import collection.JavaConverters._
    for {
      row <- rows.asScala
      cols = row.asScala
    } {
      val processorIdKeyValue = findColumn(cols, ProcessorId)
      val processorId = Bytes.toString(processorIdKeyValue.value)

      val sequenceNrKeyValue = findColumn(cols, SequenceNr)
      val sequenceNr: Long = Bytes.toLong(sequenceNrKeyValue.value)
      println(s"$processorId      $sequenceNr")
    }
  }

  case class RowWrapper(row: JArrayList[KeyValue]) extends Ordered[RowWrapper] {
    def compare(that: RowWrapper) = {
      val thisSeqNr = getSequenceNr(row)
      val thatSeqNr = getSequenceNr(that.row)
      if (thisSeqNr == thatSeqNr) 0 else if (thisSeqNr < thatSeqNr) -1 else 1
    }
  }

  def findColumn(columns: Seq[KeyValue], qualifier: Array[Byte]): KeyValue = {
    columns find { kv =>
      ju.Arrays.equals(kv.qualifier, qualifier)
    } getOrElse {
      throw new RuntimeException(s"Unable to find [${Bytes.toString(qualifier)}}] field from: ${columns.map(kv => Bytes.toString(kv.qualifier))}")
    }
  }

  def getSequenceNr(cols: Seq[KeyValue]): Long = {
    Bytes.toLong(findColumn(cols, SequenceNr).value)
  }
}

