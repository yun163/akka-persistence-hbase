package akka.persistence.hbase.common

object Const {
  val JOURNAL_CONFIG = "hbase-journal"
  val SNAPSHOT_CONFIG = "hadoop-snapshot-store"
  val ROW_KEY_PARTITION_SALT_LEN = 3
  val ROW_KEY_PRSOR_ID_LEN_MAX = 50
  val ROW_KEY_SEQ_NUM_LEN = 20
}
