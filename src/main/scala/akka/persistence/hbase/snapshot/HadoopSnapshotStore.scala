package akka.persistence.hbase.snapshot

import akka.persistence.snapshot.SnapshotStore
import akka.persistence.{SelectedSnapshot, SnapshotSelectionCriteria}
import scala.concurrent.Future
import akka.persistence.SnapshotMetadata
import akka.actor.ActorLogging

class HadoopSnapshotStore extends SnapshotStore with ActorLogging {

  val snap = HadoopSnapshotterExtensionId(context.system)

  def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = {
    log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>loadAsync<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    snap.loadAsync(processorId, criteria)
  }

  def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>saveAsync<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    snap.saveAsync(metadata, snapshot)
  }

  def saved(metadata: SnapshotMetadata): Unit = {
    log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>saved<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    snap.saved(metadata)
  }

  def delete(metadata: SnapshotMetadata): Unit = {
    log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>delete<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    snap.delete(metadata)
  }

  def delete(processorId: String, criteria: SnapshotSelectionCriteria): Unit = {
    log.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>delete1<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    snap.delete(processorId, criteria)
  }

  override def postStop(): Unit = {
    super.postStop()
    snap.postStop()
  }

}
