package org.apache.openwhisk.core.checkpoint

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.containerpool.{Container, ContainerAddress, ContainerId, Interval}
import org.apache.openwhisk.core.containerpool.docker.{DockerApiWithFileAccess, DockerContainer}
import org.apache.openwhisk.core.entity.{ByteSize, WhiskAction}
import spray.json.JsObject

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}

/**
 * A container that supports checkpointing.
 *
 * This container wraps a DockerContainer and adds checkpointing functionality.
 * When the container is destroyed, it creates a checkpoint that can be used to
 * restore the container state when the same function is invoked again.
 *
 * The container also tracks which checkpoint (if any) was used to create it,
 * so that checkpoint can be returned to the available pool when this container
 * is destroyed.
 */
class CheckpointContainer(val container: DockerContainer,
                          protected val id: ContainerId,
                          protected[core] val addr: ContainerAddress,
                          protected val functionKey: String,
                          protected val containerName: String,
                          val factory: CheckpointContainerFactory,
                          protected val usedCheckpoint: Option[Checkpoint] = None)(implicit docker: DockerApiWithFileAccess,
                                                                                   override protected val as: ActorSystem,
                                                                                   protected val ec: ExecutionContext,
                                                                                   protected val logging: Logging)
  extends Container {

  /** Obtains logs up to a given threshold from the container. Delegates to the underlying container. */
  override def logs(limit: ByteSize, waitForSentinel: Boolean)(
    implicit transid: TransactionId): Source[ByteString, Any] = {
    container.logs(limit, waitForSentinel)
  }

  /**
   * Creates a checkpoint of the container.
   *
   * This method creates a checkpoint of the container's state. The checkpoint creation
   * automatically stops the container as part of the Docker checkpoint process.
   * The checkpoint is stored in the factory's checkpoint map and can be used to
   * restore the container state when the same function is invoked again.
   *
   * If this container was restored from a checkpoint, we first stop it and then
   * return the checkpoint to the available pool with updated timestamp.
   */
  override def destroy()(implicit transid: TransactionId): Future[Unit] = {
    usedCheckpoint match {
      case Some(checkpoint) =>
        // Container was restored from a checkpoint
        // Stop the container and then return the checkpoint to the available pool
        logging.info(this, s"Stopping container ${id.asString} and returning checkpoint ${checkpoint.checkpointName} to available pool for function $functionKey")

        docker.stop(id).map { _ =>
          val checkpointToSave = checkpoint.copy(timestamp = System.currentTimeMillis())
          factory.addCheckpoint(functionKey, checkpointToSave)
          logging.info(this, s"Container stopped and checkpoint ${checkpointToSave.checkpointName} returned to available pool for function $functionKey")
        }.recoverWith {
          case e =>
            // If stop fails, try to destroy the container normally and still return the checkpoint
            logging.error(this, s"Failed to stop container: ${e.getMessage}, destroying container normally")
            container.destroy().map { _ =>
              factory.addCheckpoint(functionKey, checkpoint.copy(timestamp = System.currentTimeMillis()))
            }
        }

      case None =>
        // Container was created new, create a new checkpoint
        val checkpointName = s"checkpoint_${functionKey}_${System.currentTimeMillis()}"
        logging.info(this, s"Creating new checkpoint $checkpointName for container $containerName (${id.asString})")

        // Create the checkpoint and then add it
        factory.createCheckpoint(id, containerName, checkpointName).map { checkpoint =>
          factory.addCheckpoint(functionKey, checkpoint)
          logging.info(this, s"Container $containerName checkpointed and stopped as $checkpointName")
        }.recoverWith {
          case e =>
            logging.error(this, s"Failed to create checkpoint: ${e.getMessage}, destroying container normally")
            container.destroy()
        }
    }
  }

  override def initialize(initializer: JsObject,
                          timeout: FiniteDuration,
                          maxConcurrent: Int,
                          entity: Option[WhiskAction] = None)(implicit transid: TransactionId): Future[Interval] = {
    usedCheckpoint match {
      case Some(_) =>
        Future.successful(Interval.zero)
      case None =>
        super.initialize(initializer, timeout, maxConcurrent, entity)
    }
  }
}