package akka.persistence.hbase.journal

import akka.actor.{ ActorLogging, Actor }
import akka.persistence.PersistentRepr
import akka.persistence.hbase.common.{ Columns, RowKey, DeferredConversions, SaltedScanner, EncryptingSerializationExtension }
import akka.persistence.journal._
import org.hbase.async.KeyValue
import org.apache.hadoop.hbase.util.Bytes
import scala.annotation.switch
import scala.collection.mutable
import scala.concurrent.Future

trait HBaseAsyncRecovery extends AsyncRecovery {
  this: Actor with ActorLogging with HBaseAsyncWriteJournal =>

  import Columns._
  import RowTypeMarkers._
  import DeferredConversions._
  import collection.JavaConverters._

  // async recovery plugin impl

  // TODO: can be improved to to N parallel scans for each "partition" we created, instead of one "big scan"
  override def asyncReplayMessages(processorId: String, fromSequenceNr: Long, toSequenceNr: Long, max: Long)(replayCallback: PersistentRepr => Unit): Future[Unit] = {
    log.info(s"Async begin replay for processorId [$processorId], from sequenceNr: [$fromSequenceNr], to sequenceNr: [$toSequenceNr]")
    var retryTimes: Int = 0
    var tryStartSeqNr: Long = if (fromSequenceNr <= 0) 1 else fromSequenceNr
    var scanner: SaltedScanner = null
    val callback = replay(replayCallback, processorId) _
    var isDuplicate = false

    def hasSequenceGap(columns: mutable.Buffer[KeyValue]): Boolean = {
      val processingSeqNr = sequenceNr(columns)
      if (tryStartSeqNr != processingSeqNr) {
        if (tryStartSeqNr > processingSeqNr) {
          log.error(s"Replay $processorId Meet duplicated message: to process is $tryStartSeqNr, actual is $processingSeqNr")
          isDuplicate = true
        }
        return true
      } else {
        return false
      }
    }

    def initScanner() {
      if (scanner != null) scanner.close()
      scanner = newSaltedScanner(settings.partitionCount, serialization)
      scanner.setSaltedStartKeys(processorId, tryStartSeqNr)
      scanner.setSaltedStopKeys(processorId, RowKey.toSequenceNr(toSequenceNr))
      scanner.setKeyRegexp(processorId)
      scanner.setMaxNumRows(settings.scanBatchSize)
    }

    def handleRows(in: AnyRef): Future[Long] = in match {
      case null =>
        log.info(s"replayAsync for processorId [$processorId, $fromSequenceNr, $tryStartSeqNr] - finished!")
        scanner.close()
        Future(0L)

      case rows: AsyncBaseRows =>
        //        log.info(s"replayAsync for processorId [$processorId, $fromSequenceNr, $tryStartSeqNr] - got ${rows.size} rows...")
        for {
          row <- rows.asScala
          cols = row.asScala
        } {
          if (hasSequenceGap(cols) && retryTimes < replayGapRetry) {
            if (isDuplicate) {
              scanner.close()
              return Future.failed(new Exception(s"Replay Meet duplicated message: [$processorId, $tryStartSeqNr]"))
            }
            log.warning(s"Replay ${processorId} meet gap at ${tryStartSeqNr}")
            retryTimes += 1
            Thread.sleep(200)
            initScanner()
            return go()
          } else {
            var meetGap: Boolean = false
            var gapSeq: Long = 0L
            if (retryTimes >= replayGapRetry) {
              meetGap = true
              gapSeq = tryStartSeqNr
              if (!skipGap) {
                scanner.close()
                return Future.failed(new Exception(s"Replay failed [$processorId, $tryStartSeqNr, $replayGapRetry]"))
              }
              //            return Future(0L)
            }
            if (!meetGap && retryTimes > 0) {
              log.warning("Replay retry succeesed after $retryTimes times at $tryStartSeqNr")
            }
            val replayingId = callback(cols)
            log.info(s"Replaying $processorId at $replayingId")
            tryStartSeqNr = replayingId + 1
            if (skipGap && meetGap) {
              log.error(s"Replay ${processorId} failed by sequence gap at ${gapSeq} after ${replayGapRetry} times retry, jump to first valid seq ${tryStartSeqNr - 1}")
            }
            // re init to 0
            retryTimes = 0
          }
        }

        go()
    }

    def go() = scanner.nextRows(tryStartSeqNr == fromSequenceNr) flatMap handleRows

    initScanner
    go()
  }

  // TODO: make this multiple scans, on each partition instead of one big scan
  override def asyncReadHighestSequenceNr(processorId: String, fromSequenceNr: Long): Future[Long] = {
    // log.info(s"Async read for highest sequence number for processorId: [$processorId] (hint, seek from  nr: [$fromSequenceNr])")
    var tryStartSeqNr: Long = if (fromSequenceNr <= 0) 1 else fromSequenceNr
    val scanner = newSaltedScanner(settings.partitionCount, serialization)
    scanner.setSaltedStartKeys(processorId, fromSequenceNr)
    scanner.setSaltedStopKeys(processorId, Long.MaxValue)
    scanner.setMaxNumRows(settings.scanBatchSize)
    scanner.setKeyRegexp(processorId)

    def handleRows(in: AnyRef): Future[Long] = in match {
      case null =>
        // log.debug(s"AsyncReadHighestSequenceNr for processorId [$processorId] finished")
        scanner.close()
        Future(fromSequenceNr)

      case rows: AsyncBaseRows =>
        // log.debug(s"AsyncReadHighestSequenceNr for processorId [$processorId] - got ${rows.size} rows...")

        val maxSoFar = rows.asScala.map(cols => sequenceNr(cols.asScala)).max

        go() map {
          reachedSeqNr =>
            val rNr = math.max(reachedSeqNr, maxSoFar)
            tryStartSeqNr = rNr + 1
            rNr
        }
    }

    def go() = scanner.nextRows(tryStartSeqNr == fromSequenceNr) flatMap handleRows

    go() // map { i => println(s"HightestSequenceNr for $processorId is $i, fromSequenceNr = $fromSequenceNr"); i }
  }

  private def replay(replayCallback: (PersistentRepr) => Unit, processorId: String)(columns: mutable.Buffer[KeyValue]): Long = {
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

    try {
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

    } catch {
      case e: Exception =>
        log.error(s"Failed to get marker for $processorId , at ${msg.sequenceNr}")
        replayCallback(msg)
    }
    msg.sequenceNr
  }

  // end of async recovery plugin impl

  private def sequenceNr(columns: mutable.Buffer[KeyValue]): Long = {
    val messageKeyValue = findColumn(columns, Message)
    val msg = persistentFromBytes(messageKeyValue.value)
    msg.sequenceNr
    // Bytes.toLong(findColumn(columns, SequenceNr).value)
  }

}
