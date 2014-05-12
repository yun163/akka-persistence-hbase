package akka.persistence.hbase.journal

import akka.persistence.PersistentRepr
import akka.actor.{ ActorLogging, Actor }
import akka.persistence.journal._
import akka.persistence.hbase.common.{ Columns, RowKey, DeferredConversions }
import org.hbase.async.KeyValue
import org.hbase.async.Scanner
import org.apache.hadoop.hbase.util.Bytes
import scala.annotation.switch
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{ Success, Failure }

trait HBaseAsyncRecovery extends AsyncRecovery {
  this: Actor with ActorLogging with HBaseAsyncWriteJournal =>

  import Columns._
  import RowTypeMarkers._
  import DeferredConversions._
  import collection.JavaConverters._

  // async recovery plugin impl

  // TODO: can be improved to to N parallel scans for each "partition" we created, instead of one "big scan"
  override def asyncReplayMessages(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: PersistentRepr => Unit): Future[Unit] = {
    // log.debug(s"Async replay for processorId [$processorId], from sequenceNr: [$fromSequenceNr], to sequenceNr: [$toSequenceNr]")
    var retryTimes: Int = 0
    var tryStartSeqNr: Long = if (fromSequenceNr <= 0) 1 else fromSequenceNr
    var scanner: Scanner = null
    val callback = replay(replayCallback) _

    def hasSequenceGap(columns: Seq[KeyValue]): Boolean = {
      val sequenceNrKeyValue = findColumn(columns, SequenceNr)
      if (tryStartSeqNr != Bytes.toLong(sequenceNrKeyValue.value)) {
        return true
      } else {
        tryStartSeqNr += 1
        return false
      }
    }

    def initScanner() {
      if (scanner != null) scanner.close()
      scanner = newScanner()
      scanner.setStartKey(RowKey(processorId, tryStartSeqNr).toBytes)
      scanner.setStopKey(RowKey.toKeyForProcessor(processorId, toSequenceNr))
      scanner.setKeyRegexp(RowKey.patternForProcessor(processorId))
      scanner.setMaxNumRows(settings.scanBatchSize)
    }

    def handleRows(in: AnyRef): Future[Long] = in match {
      case null =>
        // log.debug(s"replayAsync for processorId [$processorId] - finished!")
        scanner.close()
        Future(0L)

      case rows: AsyncBaseRows =>
        // log.debug(s"replayAsync for processorId [$processorId] - got ${rows.size} rows...")
        for {
          row <- rows.asScala
          cols = row.asScala
        } {
          if (hasSequenceGap(cols) && retryTimes < replayGapRetry) {
            retryTimes += 1
            Thread.sleep(100)
            initScanner()
            return go()
          } else {
            if (retryTimes > replayGapRetry) {
              log.warning(s"processorId : ${processorId}, with view gap at ${tryStartSeqNr} after ${replayGapRetry} times retry")
            }
            callback(cols)
          }
        }
        // re init to 0
        retryTimes = 0
        go()
    }

    def go() = scanner.nextRows() flatMap handleRows

    initScanner
    go()
  }

  // TODO: make this multiple scans, on each partition instead of one big scan
  override def asyncReadHighestSequenceNr(processorId: String, fromSequenceNr: Long): Future[Long] = {
    // log.debug(s"Async read for highest sequence number for processorId: [$processorId] (hint, seek from  nr: [$fromSequenceNr])")

    val scanner = newScanner()
    scanner.setStartKey(RowKey(processorId, fromSequenceNr).toBytes)
    scanner.setStopKey(RowKey.lastForProcessor(processorId))
    scanner.setKeyRegexp(RowKey.patternForProcessor(processorId))

    def handleRows(in: AnyRef): Future[Long] = in match {
      case null =>
        // log.debug(s"AsyncReadHighestSequenceNr for processorId [$processorId] finished")
        scanner.close()
        Future(0)

      case rows: AsyncBaseRows =>
        // log.debug(s"AsyncReadHighestSequenceNr for processorId [$processorId] - got ${rows.size} rows...")

        val maxSoFar = rows.asScala.map(cols => sequenceNr(cols.asScala)).max

        go() map {
          reachedSeqNr =>
            math.max(reachedSeqNr, maxSoFar)
        }
    }

    def go() = scanner.nextRows() flatMap handleRows

    go()
  }

  private def replay(replayCallback: (PersistentRepr) => Unit)(columns: mutable.Buffer[KeyValue]): Long = {
    val messageKeyValue = findColumn(columns, Message)
    var msg = persistentFromBytes(messageKeyValue.value)
    if (enableExportSequence) {
      val processorIdKeyValue = findColumn(columns, ProcessorId)
      val processorId = Bytes.toString(processorIdKeyValue.value)

      val sequenceNrKeyValue = findColumn(columns, SequenceNr)
      val sequenceNr: Long = Bytes.toLong(sequenceNrKeyValue.value)

      if (processorId.equals(exportProcessorId)) {
        printerWriter.println(sequenceNr + " " + messageKeyValue.value.length)
        printerWriter.flush()
      }
    }

    val markerKeyValue = findColumn(columns, Marker)
    val marker = Bytes.toString(markerKeyValue.value)

    // TODO: make this a @switch
    (markerKeyValue.value.head.toChar: @switch) match {
      case 'A' =>
        replayCallback(msg)

      case 'D' =>
        msg = msg.update(deleted = true)

      case 'S' =>
        // thanks to treating Snapshot rows as deleted entries, we won't suddenly apply a Snapshot() where the
        // our Processor expects a normal message. This is implemented for the HBase backed snapshot storage,
        // if you use the HDFS storage there won't be any snapshot entries in here.
        // As for message deletes: if we delete msgs up to seqNr 4, and snapshot was at 3, we want to delete it anyway.
        msg = msg.update(deleted = true)

      case _ =>
        val channelId = extractSeqNrFromConfirmedMarker(marker)
        msg = msg.update(confirms = channelId +: msg.confirms)
        replayCallback(msg)
    }

    msg.sequenceNr
  }

  // end of async recovery plugin impl

  private def sequenceNr(columns: mutable.Buffer[KeyValue]): Long = {
    val messageKeyValue = findColumn(columns, Message)
    val msg = persistentFromBytes(messageKeyValue.value)
    msg.sequenceNr
  }

}
