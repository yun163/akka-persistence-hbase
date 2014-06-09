package akka.persistence.hbase.journal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import com.typesafe.config._
import org.apache.hadoop.hbase.{ HColumnDescriptor, HTableDescriptor }
import akka.persistence.hbase.common.Const._

object HBaseJournalInit {

  import Bytes._
  import collection.JavaConverters._

  /**
   * Creates (or adds column family to existing table) table to be used with HBaseJournal.
   *
   * @return true if a modification was run on hbase (table created or family added)
   */
  def createTable(config: Config, persistentConfig: String): Boolean = {
    val conf = getHBaseConfig(config, persistentConfig)
    val admin = new HBaseAdmin(conf)

    val journalConfig = config.getConfig(persistentConfig)
    val table = journalConfig.getString("table")
    val familyName = journalConfig.getString("family")
    val partitionCount = PARTITION_COUNT // Integer.parseInt(journalConfig.getString("partition.count"))

    try doInitTable(admin, table, familyName, partitionCount) finally admin.close()
  }

  private def doInitTable(admin: HBaseAdmin, tableName: String, familyName: String, partitionCount: Int): Boolean = {
    if (admin.tableExists(tableName)) {
      val tableDesc = admin.getTableDescriptor(toBytes(tableName))
      if (tableDesc.getFamily(toBytes(familyName)) == null) {
        // target family does not exists, will add it.
        admin.addColumn(tableName, new HColumnDescriptor(familyName))
        true
      } else {
        // existing table is OK, no modifications run.
        false
      }
    } else {
      val tableDesc = new HTableDescriptor(toBytes(tableName))
      val familyDesc = genColumnFamily(toBytes(familyName), 1)
      tableDesc.addFamily(familyDesc)
      if (partitionCount > 1) {
        val splitPoints = getSplitKeys(partitionCount);
        admin.createTable(tableDesc, splitPoints)
      } else {
        admin.createTable(tableDesc)
      }
      true
    }
  }

  /**
   * Construct Configuration, passing in all `hbase.*` keys from the typesafe Config.
   */
  def getHBaseConfig(config: Config, persistenceConfig: String): Configuration = {
    val c = new Configuration()
    @inline def hbaseKey(s: String) = "hbase." + s

    val persistenseConfig = config.getConfig(persistenceConfig)
    val hbaseConfig = persistenseConfig.getConfig("hbase")

    // TODO: does not cover all cases
    hbaseConfig.entrySet().asScala foreach { e =>
      c.set(hbaseKey(e.getKey), e.getValue.unwrapped.toString)
    }
    c
  }

  private def genColumnFamily(family: Array[Byte], columnMaxVersion: Int): HColumnDescriptor = {
    val familyDesc: HColumnDescriptor = new HColumnDescriptor(family)
      .setInMemory(false)
      .setMaxVersions(columnMaxVersion);
    familyDesc
  }

  private def getSplitKeys(splitNum: Int, isPrint: Boolean = false): Array[Array[Byte]] = {
    val list = collection.mutable.ListBuffer.empty[Array[Byte]]
    for (i <- 1 until splitNum) {
      val keyBytes = collection.mutable.ListBuffer.empty[Byte]
      keyBytes ++= Bytes.toBytes(padNum(i, 2))
      val zeroByte: Byte = Bytes.toBytes(0).tail(0)
      for (j <- 0 until 24) {
        keyBytes += zeroByte
      }
      val bytes = keyBytes.toArray
      if (isPrint) println(s" $i ${Bytes.toString(bytes)} ${renderBytes(bytes)}")
      list.append(bytes)
    }
    list.toArray
  }

  def padNum(l: Int, howLong: Int): String = String.valueOf(l).reverse.padTo(howLong, "0").reverse.mkString.substring(0, howLong)

  def renderBytes(bytes: Array[Byte]): String = {
    bytes.map("%02x".format(_)).mkString
  }
}