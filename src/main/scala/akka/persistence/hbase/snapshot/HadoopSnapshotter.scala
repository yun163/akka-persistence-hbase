package akka.persistence.hbase.snapshot

import akka.actor.{ ActorSystem, Extension }
import akka.persistence.{ SelectedSnapshot, SnapshotSelectionCriteria, SnapshotMetadata }
import akka.persistence.serialization.Snapshot
import akka.persistence.hbase.common.EncryptingSerializationExtension
import akka.persistence.hbase.journal.PluginPersistenceSettings
import scala.concurrent.Future
import scala.util.Try

/**
 * Common API for Snapshotter implementations. Used to provide an interface for the Extension.
 */
trait HadoopSnapshotter extends Extension {

  def system: ActorSystem
  val settings: PluginPersistenceSettings

  protected lazy val serialization = EncryptingSerializationExtension(system, system.settings.config.getString("akka.persistence.encryption-settings"))

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]]

  def saveAsync(meta: SnapshotMetadata, snapshot: Any): Future[Unit]

  def saved(metadata: SnapshotMetadata): Unit

  def delete(metadata: SnapshotMetadata): Unit

  def delete(processorId: String, criteria: SnapshotSelectionCriteria): Unit

  protected def deserialize(bytes: Array[Byte]): Try[Snapshot] =
    Try(serialization.deserialize(bytes, classOf[Snapshot]))

  protected def serialize(snapshot: Snapshot): Try[Array[Byte]] =
    Try(serialization.serialize(snapshot))

  def postStop(): Unit
}
