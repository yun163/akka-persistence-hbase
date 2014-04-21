package akka.persistence.hbase.journal

import akka.persistence.PersistenceSettings
import java.util.concurrent.atomic.AtomicReference
import org.hbase.async.HBaseClient

object HBaseClientFactory {

  /** based on the docs, there should always be only one instance, reused even if we had more tables */
  private val client = new AtomicReference[HBaseClient]()

  /** Always returns the same client */
  def getClient(config: PluginPersistenceSettings, persistenceSettings: PersistenceSettings): HBaseClient = {
    client.compareAndSet(null, new HBaseClient(config.zookeeperQuorum))

    // since we will be forcing a flush anyway after each batch, let's not make asyncbase flush more than it needs to.
    // for example, we tell akka "200", but asyncbase was set to "20", so it would flush way more often than we'd expect it to.
    // by setting the internal flushing to max(...), we're manually in hold of doing the flushing at the rigth moment.
    val maxBatchSize = List(
      persistenceSettings.journal.maxMessageBatchSize,
      persistenceSettings.journal.maxConfirmationBatchSize,
      persistenceSettings.journal.maxDeletionBatchSize
    ).max.toShort
    val hbaseClient = client.get()
    // set Flush Interval to 0 to disable the batch flush, will flush instantly
    hbaseClient.setFlushInterval(maxBatchSize)
    hbaseClient
  }

  def shutDown() {
    if (client.get() != null) {
      val hbaseClient = client.get()
      client set null
      try {
        hbaseClient.shutdown()
      } catch {
        case e: Exception => System.err.println(s"Caught Exception in hbaseClient shutdown : ${e.getMessage}")
      }
    }
  }

}
