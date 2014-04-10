package akka.persistence.hbase.snapshot

import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{ SelectedSnapshot, SnapshotSelectionCriteria }
import scala.concurrent.Future
import akka.persistence.SnapshotMetadata
import akka.actor.ActorLogging

class HadoopSnapshotStore extends SnapshotStore with ActorLogging {

  val snap = HadoopSnapshotterExtensionId(context.system)

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    log.info(s"loadAsync started --- processorId: ${processorId}, criteria: $criteria{}")
    snap.loadAsync(processorId, criteria)

  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    log.info(s"saveAsync started --- metadata: ${metadata}, snapshotClass: ${snapshot.getClass.getName}")
    snap.saveAsync(metadata, snapshot)
  }

  def saved(metadata: SnapshotMetadata): Unit = {
    snap.saved(metadata)
    log.info(s"saved completed --- metadata: ${metadata}")
  }

  def delete(metadata: SnapshotMetadata): Unit = {
    snap.delete(metadata)
    log.info(s"delete completed --- metadata: ${metadata}")
  }

  def delete(processorId: String, criteria: SnapshotSelectionCriteria): Unit = {
    snap.delete(processorId, criteria)
    log.info(s"delete completed --- processorId: ${processorId}, criteria: ${criteria}")
  }

  override def postStop(): Unit = {
    super.postStop()
    snap.postStop()
  }

}
