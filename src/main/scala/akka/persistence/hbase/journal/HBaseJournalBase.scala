package akka.persistence.hbase.journal

import akka.actor.{ Actor, ActorLogging }
import akka.persistence.hbase.common.{ AsyncBaseUtils, HBaseSerialization }
import java.util.{ ArrayList => JArrayList }
import org.hbase.async.{ HBaseClient, KeyValue }
import org.apache.hadoop.conf.Configuration

// TODO: split into one API classes and register the impls as extensions
trait HBaseJournalBase extends HBaseSerialization with AsyncBaseUtils {
  this: Actor with ActorLogging =>

  val settings: PluginPersistenceSettings
  val client: HBaseClient
  def hadoopConfig: Configuration

  override def getTable = settings.table
  override def getFamily = settings.family

  type AsyncBaseRows = JArrayList[JArrayList[KeyValue]]

  /** Used to avoid writing all data to the same region - see "hot region" problem */
  def partition(sequenceNr: Long): Long = sequenceNr % settings.partitionCount

}
