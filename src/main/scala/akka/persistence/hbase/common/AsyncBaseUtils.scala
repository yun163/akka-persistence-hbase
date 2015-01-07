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
  val scanners: Seq[Scanner] = for (part <- 0 until partitionCount) yield {
    newPlainScanner()
  }
  val TRY_STEP = 3
  val CHANCE_THRESHOLD = 10
  var innerStartNr: Long = 0L
  var innerProcessorId: String = ""

  private def newPlainScanner() = {
    val scanner = client.newScanner(table)
    scanner.setFamily(family)
    scanner
  }

  def setSaltedStartKeys(processorId: String, startSequenceNr: Long) {
    innerStartNr = startSequenceNr
    for (part <- 0 until partitionCount) scanners(part).setStartKey(SaltedRowKey(processorId, startSequenceNr, part).toBytes)
  }

  def setSaltedStopKeys(processorId: String, stopSequenceNr: Long) {
    this.innerProcessorId = processorId
    for (part <- 0 until partitionCount) scanners(part).setStopKey(SaltedRowKey(processorId, stopSequenceNr, part).toBytes)
  }

  def setKeyRegexp(processorId: String) {
    for (part <- 0 until partitionCount) scanners(part).setKeyRegexp(RowKey.patternForProcessor(processorId))
  }

  def setMaxNumRows(batchSize: Int) {
    for (part <- 0 until partitionCount) scanners(part).setMaxNumRows(batchSize)
  }

  private def innerNextRows(scanner: Scanner): Future[AsyncBaseRows] = {
    scanner.nextRows()
  }

  def nextRows(nrows: Int): Future[AsyncBaseRows] = {
    setMaxNumRows(nrows)
    nextRows()
  }

  def nextRows(isStart: Boolean = false): Future[AsyncBaseRows] = {
    val futures = isStart match {
      // isStart true means this this the first query for the startSeqNr
      // check first whether there is event at the first partition,
      // if not exists event, will no need to query other partitions
      case true =>
        // try multi partition by chance to avoid first event missed result in replay failed,
        // as first event miss condition looks like no more events left, but the fact is not
        val byChance = ((new scala.util.Random()).nextInt() % CHANCE_THRESHOLD) == 1
        val startParts: Seq[Int] =
          0 until (if (byChance) TRY_STEP else 1) map {
            i =>
              ((partitionCount + innerStartNr + i - 1) % partitionCount).toInt
          }
        val tryResults = (0 until startParts.size) map {
          i =>
            innerNextRows(scanners(startParts(i)))
        }
        sequenceFutures(tryResults) map {
          case rows: AsyncBaseRows if rows.nonEmpty =>
            //            logger.info(s"nextRows for $innerProcessorId, $isStart, $byChance, $startParts get ${rows.size()} rows")
            val partsSet = startParts.toSet
            var returnTrys = false
            (0 until partitionCount) map {
              part =>
                if (partsSet.contains(part)) {
                  if (!returnTrys) {
                    returnTrys = true
                    Future(rows)
                  } else {
                    Future(new AsyncBaseRows)
                  }
                } else {
                  innerNextRows(scanners(part))
                }
            }
          case _ =>
            //            logger.info(s"nextRows for $innerProcessorId, $isStart, $byChance, $startParts get no rows")
            (0 until partitionCount) map {
              part =>
                Future(new AsyncBaseRows)
            }
        }
      case _ =>
        Future(null) map {
          f =>
            //            logger.info(s"nextRows for $innerProcessorId, $isStart")
            (0 until partitionCount) map {
              part =>
                innerNextRows(scanners(part))
            }
        }
    }
    futures flatMap {
      fs =>
        sequenceFutures(fs)
    }
  }

  def close() {
    scanners map {
      _.close()
    }
  }

  private def sequenceFutures(fs: Seq[Future[AsyncBaseRows]]): Future[AsyncBaseRows] = {
    Future.sequence(fs) map {
      case rowsList: Seq[AsyncBaseRows] =>
        val list = new AsyncBaseRows
        if (rowsList != null && rowsList.size > 0) {
          for (part <- 0 until rowsList.size) {
            if (rowsList(part) != null && rowsList(part).size() > 0) {
              list.addAll(rowsList(part))
            }
          }
        }
        if (list.isEmpty) {
          null
        } else {
          val rows: Seq[RowWrapper] = list map {
            row => RowWrapper(row)
          }
          val rows1 = rows.toArray
          Sorting.quickSort(rows1)
          val arr = new AsyncBaseRows
          rows1.foreach(wrapper => arr.add(wrapper.row))
          //  renderList(arr)
          arr
        }
      case _ => null
    }
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
    columns find {
      kv =>
        ju.Arrays.equals(kv.qualifier, qualifier)
    } getOrElse {
      throw new RuntimeException(s"Unable to find [${Bytes.toString(qualifier)}}] field from: ${columns.map(kv => Bytes.toString(kv.qualifier))}")
    }
  }

  def getSequenceNr(cols: Seq[KeyValue]): Long = {
    Bytes.toLong(findColumn(cols, SequenceNr).value)
  }
}

