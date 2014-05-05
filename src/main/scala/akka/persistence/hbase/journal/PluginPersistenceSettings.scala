package akka.persistence.hbase.journal

import com.typesafe.config.Config

/**
 *
 * @param table table to be used to store akka messages
 * @param family column family name to store akka messages in
 * @param partitionCount Number of regions the used Table is partitioned to.
 *                       Currently must be FIXED, and not change during the lifetime of the app.
 *                       Should be a bigger number, for example 10 even if you currently have 2 regions, so you can split regions in the future.
 * @param scanBatchSize when performing scans, how many items to we want to obtain per one next(N) call
 * @param replayDispatcherId dispatcher for fetching and replaying messages
 */
case class PluginPersistenceSettings(
  zookeeperQuorum: String,
  table: String,
  family: String,
  partitionCount: Int,
  scanBatchSize: Int,
  clientFlushInterval: Int,
  pluginDispatcherId: String,
  replayDispatcherId: String,
  publishTestingEvents: Boolean,
  snapshotHdfsDir: String,
  hdfsDefaultName: String)

object PluginPersistenceSettings {
  def apply(rootConfig: Config, persistenceConf: String): PluginPersistenceSettings = {
    val persistenceConfig = rootConfig.getConfig(persistenceConf)
    def getString(path: String): String = persistenceConfig.getString(path)
    def getInt(path: String): Int = persistenceConfig.getInt(path)
    def getBoolean(path: String): Boolean = persistenceConfig.getBoolean(path)
    def withCheck[T](path: String, default: T)(getConfig: String => T): T = if (persistenceConfig.hasPath(path)) getConfig(path) else default
    PluginPersistenceSettings(
      zookeeperQuorum = withCheck("hbase.zookeeper.quorum", "localhost:2181")(getString),
      table = withCheck("table", "")(getString),
      family = withCheck("family", "")(getString),
      partitionCount = withCheck("partition.count", 1)(getInt),
      scanBatchSize = withCheck("scan-batch-size", 50)(getInt),
      clientFlushInterval = withCheck("client-flush-interval", 0)(getInt),
      pluginDispatcherId = withCheck("plugin-dispatcher", "")(getString),
      replayDispatcherId = withCheck("replay-dispatcher", "")(getString),
      publishTestingEvents = withCheck("publish-testing-events", false)(getBoolean),
      snapshotHdfsDir = withCheck("snapshot-dir", "")(getString),
      hdfsDefaultName = withCheck("hdfs-default-name", "")(getString)
    )
  }
}
