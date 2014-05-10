package akka.persistence.hbase.journal

import akka.actor.{ Actor, ActorLogging }
import akka.persistence.hbase.common._
import akka.persistence.hbase.common.Const._
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ PersistenceSettings, PersistentConfirmation, PersistentId, PersistentRepr }
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.immutable
import scala.concurrent._
import scala.concurrent.duration._

/**
 * Asyncronous HBase Journal.
 *
 * Uses AsyncBase to implement asynchronous IPC with HBase.
 */
class HBaseAsyncWriteJournal extends Actor with ActorLogging
    with HBaseJournalBase with AsyncWriteJournal
    with HBaseAsyncRecovery {

  import RowTypeMarkers._
  import TestingEventProtocol._

  private lazy val config = context.system.settings.config

  implicit override lazy val settings = PluginPersistenceSettings(config, JOURNAL_CONFIG)

  lazy val hadoopConfig = HBaseJournalInit.getHBaseConfig(config, JOURNAL_CONFIG)

  lazy val client = HBaseClientFactory.getClient(settings, new PersistenceSettings(config.getConfig("akka.persistence")))

  lazy val publishTestingEvents = settings.publishTestingEvents

  implicit override val executionContext = context.system.dispatchers.lookup(settings.pluginDispatcherId)

  HBaseJournalInit.createTable(config, Const.JOURNAL_CONFIG)

  import Bytes._
  import Columns._
  import DeferredConversions._
  import collection.JavaConverters._

  // journal plugin api impl -------------------------------------------------------------------------------------------

  override def asyncWriteMessages(persistentBatch: immutable.Seq[PersistentRepr]): Future[Unit] = {
    // log.debug(s"Write async for ${persistentBatch.size} presistent messages")

    persistentBatch map { p =>
      import p._

      val f = executePut(
        RowKey(processorId, sequenceNr).toBytes,
        Array(ProcessorId, SequenceNr, Marker, Message),
        Array(toBytes(processorId), toBytes(sequenceNr), toBytes(AcceptedMarker), persistentToBytes(p)),
        true // forceFlush to guarantee ordering
      )
      Await.result(f, 10 seconds)
    }

    Future(())
  }

  override def asyncWriteConfirmations(confirmations: immutable.Seq[PersistentConfirmation]): Future[Unit] = {
    // log.debug(s"AsyncWriteConfirmations for ${confirmations.size} messages")

    val fs = confirmations map { confirm =>
      confirmAsync(confirm.processorId, confirm.sequenceNr, confirm.channelId)
    }
    Future.sequence(fs) map { case _ => flushWrites() }
  }

  override def asyncDeleteMessages(messageIds: immutable.Seq[PersistentId], permanent: Boolean): Future[Unit] = {
    // log.debug(s"Async delete [${messageIds.size}] messages, premanent: $permanent")

    val doDelete = deleteFunctionFor(permanent)

    val deleteFutures = for {
      messageId <- messageIds
      rowId = RowKey(messageId.processorId, messageId.sequenceNr)
    } yield doDelete(rowId.toBytes)

    Future.sequence(deleteFutures) map { case _ => flushWrites() }
  }

  override def asyncDeleteMessagesTo(processorId: String, toSequenceNr: Long, permanent: Boolean): Future[Unit] = {
    // log.debug(s"AsyncDeleteMessagesTo for processorId: [$processorId] to sequenceNr: $toSequenceNr, premanent: $permanent")
    val doDelete = deleteFunctionFor(permanent)

    val scanner = newScanner()
    scanner.setStartKey(RowKey.firstForProcessor(processorId).toBytes)
    scanner.setStopKey(RowKey.toKeyForProcessor(processorId, toSequenceNr))
    scanner.setKeyRegexp(RowKey.patternForProcessor(processorId))

    def handleRows(in: AnyRef): Future[Unit] = in match {
      case null =>
        //  log.debug("AsyncDeleteMessagesTo finished scanning for keys")
        flushWrites()
        scanner.close()
        Future(Array[Byte]())

      case rows: AsyncBaseRows =>
        val deletes = for {
          row <- rows.asScala
          col <- row.asScala.headOption // just one entry is enough, because is contains the key
        } yield doDelete(col.key)

        go() flatMap { _ => Future.sequence(deletes) }
    }

    def go() = scanner.nextRows() flatMap handleRows
    go()
  }

  // end of journal plugin api impl ------------------------------------------------------------------------------------

  def confirmAsync(processorId: String, sequenceNr: Long, channelId: String): Future[Unit] = {
    // log.debug(s"Confirming async for processorId: [$processorId], sequenceNr: $sequenceNr and channelId: $channelId")
    executePut(
      RowKey(processorId, sequenceNr).toBytes,
      Array(Marker),
      Array(confirmedMarkerBytes(channelId)),
      false // not to flush immediately
    )
  }

  private def deleteFunctionFor(permanent: Boolean): (Array[Byte]) => Future[Unit] = {
    if (permanent) deleteRow
    else markRowAsDeleted
  }

  override def postStop(): Unit = {
    // client could be shutdown once at here, another user: HBaseSnapshotter should not shut down it,
    // for it may still be used here
    HBaseClientFactory.shutDown()
    super.postStop()
  }
}
