package akka.persistence.hbase.journal

import akka.actor.{ Actor, ActorLogging }
import akka.persistence.hbase.common._
import akka.persistence.hbase.common.Const._
import akka.persistence.journal.AsyncWriteJournal
import akka.persistence.{ PersistenceSettings, PersistentConfirmation, PersistentId, PersistentRepr }
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.immutable
import scala.concurrent._
import java.io.PrintWriter
import akka.event.LoggingAdapter

/**
 * Asyncronous HBase Journal.
 *
 * Uses AsyncBase to implement asynchronous IPC with HBase.
 */
class HBaseAsyncWriteJournal extends Actor with ActorLogging
    with HBaseJournalBase with AsyncWriteJournal
    with HBaseAsyncRecovery {

  import RowTypeMarkers._

  override implicit val logger: LoggingAdapter = log

  private lazy val config = context.system.settings.config

  implicit override lazy val settings = PluginPersistenceSettings(config, JOURNAL_CONFIG)

  lazy val hadoopConfig = HBaseJournalInit.getHBaseConfig(config, JOURNAL_CONFIG)

  lazy val client = HBaseClientFactory.getClient(settings, new PersistenceSettings(config.getConfig("akka.persistence")))

  val enableExportSequence: Boolean = config.getBoolean("akka.persistence.export-sequence.enable-export")
  val exportProcessorId: String = config.getString("akka.persistence.export-sequence.processor-id")
  val exportSequenceFile: String = config.getString("akka.persistence.export-sequence.file")
  var printerWriter: java.io.PrintWriter = null
  val replayGapRetry: Int = config.getInt("akka.persistence.replay-gap-retry")

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
      //      println(RowKey(processorId, sequenceNr).toKeyString)
      executePut(
        RowKey(processorId, sequenceNr).toBytes,
        Array(ProcessorId, SequenceNr, Marker, Message),
        Array(toBytes(processorId), toBytes(sequenceNr), toBytes(AcceptedMarker), persistentToBytes(p)),
        false // forceFlush to guarantee ordering
      )
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

    val scanner = newSaltedScanner(settings.partitionCount)
    scanner.setSaltedStartKeys(processorId, 1)
    scanner.setSaltedStopKeys(processorId, RowKey.toSequenceNr(toSequenceNr))
    scanner.setMaxNumRows(settings.scanBatchSize)
    scanner.setKeyRegexp(processorId)

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

  override def preStart(): Unit = {
    if (enableExportSequence)
      printerWriter = new PrintWriter(new java.io.File(exportSequenceFile))
  }

  override def postStop(): Unit = {
    // client could be shutdown once at here, another user: HBaseSnapshotter should not shut down it,
    // for it may still be used here
    if (enableExportSequence)
      printerWriter.close()
    HBaseClientFactory.shutDown()
    super.postStop()
  }
}
