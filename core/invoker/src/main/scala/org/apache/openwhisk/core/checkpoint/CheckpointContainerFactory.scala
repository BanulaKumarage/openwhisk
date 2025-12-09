package org.apache.openwhisk.core.checkpoint

import akka.actor.ActorSystem
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.core.containerpool.docker.{DockerApiWithFileAccess, DockerContainer, RuncApi}
import org.apache.openwhisk.core.containerpool.{Container, ContainerArgsConfig, ContainerFactory, ContainerId, RuntimesRegistryConfig}
import org.apache.openwhisk.core.entity.{ByteSize, ExecManifest, InvokerInstanceId}
import pureconfig.loadConfigOrThrow
import pureconfig._
import pureconfig.generic.auto._

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

case class CheckpointConfig(
                             enabled: Boolean = true,
                             maxCheckpoints: Int = 100,
                             maxCheckpointsPerFunction: Int = 5)

/**
 * Represents a checkpoint entry.
 *
 * @param checkpointName The name of the checkpoint
 * @param containerName The name of the container that was checkpointed
 * @param timestamp The time when the checkpoint was created
 */
case class Checkpoint(checkpointName: String, containerName: String, timestamp: Long)

/**
 * A container factory that implements container checkpointing and restoration.
 *
 * This factory creates containers and manages checkpoints for them. When a container
 * is destroyed, it creates a checkpoint that can be used to restore the container
 * state when the same function is invoked again.
 */
class CheckpointContainerFactory(
                                  instance: InvokerInstanceId,
                                  parameters: Map[String, Set[String]],
                                  containerArgsConfig: ContainerArgsConfig = loadConfigOrThrow[ContainerArgsConfig](ConfigKeys.containerArgs),
                                  checkpointConfig: CheckpointConfig = loadConfigOrThrow[CheckpointConfig]("whisk.container-pool.checkpoint-container"),
                                  protected val runtimesRegistryConfig: RuntimesRegistryConfig =
                                  loadConfigOrThrow[RuntimesRegistryConfig](ConfigKeys.runtimesRegistry),
                                  protected val userImagesRegistryConfig: RuntimesRegistryConfig =
                                  loadConfigOrThrow[RuntimesRegistryConfig](ConfigKeys.userImagesRegistry))(implicit actorSystem: ActorSystem,
                                                                                                            ec: ExecutionContext,
                                                                                                            logging: Logging,
                                                                                                            docker: DockerApiWithFileAccess,
                                                                                                            runc: RuncApi)
  extends ContainerFactory {

  // Map to store list of checkpoints by function name
  private val checkpoints = mutable.Map[String, mutable.ListBuffer[Checkpoint]]()

  override def init(): Unit = {
    cleanup()
  }

  /**
   * Creates a checkpoint for a container.
   *
   * @param containerId The ID of the container to checkpoint
   * @param containerName The name of the container to checkpoint
   * @param checkpointName The name to give the checkpoint
   * @return A Future containing the checkpoint object
   */
  def createCheckpoint(containerId: ContainerId, containerName: String, checkpointName: String): Future[Checkpoint] = {
    implicit val transid = TransactionId.invokerNanny

    logging.info(this, s"Creating checkpoint $checkpointName for container $containerName (${containerId.asString})")

    docker.checkpointCreate(containerId, checkpointName).map { _ =>
      val checkpoint = Checkpoint(checkpointName, containerName, System.currentTimeMillis())
      logging.info(this, s"Created checkpoint $checkpointName for container $containerName")
      checkpoint
    }.recoverWith {
      case e =>
        logging.error(this, s"Failed to create checkpoint $checkpointName for container $containerName: ${e.getMessage}")
        Future.failed(e)
    }
  }

  /**
   * Restores a container from a checkpoint.
   *
   * @param checkpoint The checkpoint object containing checkpoint name and container name
   * @return A Future completing when the container is restored
   */
  def restoreFromCheckpoint(checkpoint: Checkpoint): Future[ContainerId] = {
    implicit val transid = TransactionId.invokerNanny

    logging.info(this, s"Restoring container ${checkpoint.containerName} from checkpoint ${checkpoint.checkpointName}")

    docker.startWithCheckpoint(ContainerId(checkpoint.containerName), checkpoint.checkpointName).map { _ =>
      logging.info(this, s"Restored container ${checkpoint.containerName} from checkpoint ${checkpoint.checkpointName}")
      ContainerId(checkpoint.containerName)
    }.recoverWith {
      case e =>
        logging.error(this, s"Failed to restore container ${checkpoint.containerName} from checkpoint ${checkpoint.checkpointName}: ${e.getMessage}")
        Future.failed(e)
    }
  }

  /**
   * Gets a function key for the checkpoint map.
   *
   * @param actionImage The action image
   * @return A string key for the checkpoint map
   */
  private def getFunctionKey(actionImage: ExecManifest.ImageName): String = {
    actionImage.name.toString
  }

  /**
   * Gets and removes the most recent checkpoint for a given function key.
   * The checkpoint is removed from the list because it's being used and cannot
   * be reused until the container using it is destroyed.
   *
   * @param functionKey The function key
   * @return An Option containing the checkpoint object if available
   */
  def getCheckpoint(functionKey: String): Option[Checkpoint] = {
    checkpoints.get(functionKey).flatMap { list =>
      if (list.nonEmpty) {
        // Get and remove the most recent checkpoint (last one in the list)
        val checkpoint = list.remove(list.size - 1)
        logging.info(this, s"Retrieved checkpoint ${checkpoint.checkpointName} for function $functionKey (removed from available list)")

        // Clean up empty lists
        if (list.isEmpty) {
          checkpoints.remove(functionKey)
        }

        Some(checkpoint)
      } else {
        None
      }
    }
  }

  /**
   * Creates a container, potentially restoring from a checkpoint if available.
   */
  override def createContainer(
                                tid: TransactionId,
                                name: String,
                                actionImage: ExecManifest.ImageName,
                                userProvidedImage: Boolean,
                                memory: ByteSize,
                                cpuShares: Int,
                                cpuLimit: Option[Double])(implicit config: WhiskConfig, logging: Logging): Future[Container] = {

    val functionKey = getFunctionKey(actionImage)

    // Check if we have a checkpoint for this function
    getCheckpoint(functionKey) match {
      case Some(checkpoint) if checkpointConfig.enabled =>
        logging.info(this, s"Found checkpoint ${checkpoint.checkpointName} for function $functionKey, attempting restore")

        // Try to restore from checkpoint
        restoreFromCheckpoint(checkpoint).flatMap { containerId =>
          // Get the container's IP address
          docker.inspectIPAddress(containerId, containerArgsConfig.network)(tid).flatMap { ip =>
            // Create a new DockerContainer
            val container = new DockerContainer(containerId, ip, false)
            // Wrap it in a CheckpointContainer, passing the checkpoint that was used
            Future.successful(new CheckpointContainer(container, containerId, ip, functionKey, checkpoint.containerName, this, Some(checkpoint)))
          }
        }.recoverWith {
          case e =>
            // If restoration fails, return the checkpoint to the pool and create a new container
            logging.warn(this, s"Failed to restore from checkpoint ${checkpoint.checkpointName}, returning to pool and creating new container: ${e.getMessage}")
            addCheckpoint(functionKey, checkpoint)
            createNewContainer(tid, name, actionImage, userProvidedImage, memory, cpuShares, cpuLimit)
        }

      case _ =>
        // No checkpoint available, create a new container
        createNewContainer(tid, name, actionImage, userProvidedImage, memory, cpuShares, cpuLimit)
    }
  }

  /**
   * Creates a new container without using a checkpoint.
   */
  private def createNewContainer(
                                  tid: TransactionId,
                                  name: String,
                                  actionImage: ExecManifest.ImageName,
                                  userProvidedImage: Boolean,
                                  memory: ByteSize,
                                  cpuShares: Int,
                                  cpuLimit: Option[Double])(implicit config: WhiskConfig, logging: Logging): Future[Container] = {

    val registryConfig =
      ContainerFactory.resolveRegistryConfig(userProvidedImage, runtimesRegistryConfig, userImagesRegistryConfig)
    val image = if (userProvidedImage) Left(actionImage) else Right(actionImage)

    logging.info(this, s"Creating new container for ${actionImage.name}")

    // Create a new container
    val container = DockerContainer.create(
      tid,
      image = image,
      registryConfig = Some(registryConfig),
      memory = memory,
      cpuShares = cpuShares,
      cpuLimit = cpuLimit,
      environment = Map("__OW_API_HOST" -> config.wskApiHost) ++ containerArgsConfig.extraEnvVarMap,
      network = containerArgsConfig.network,
      dnsServers = containerArgsConfig.dnsServers,
      dnsSearch = containerArgsConfig.dnsSearch,
      dnsOptions = containerArgsConfig.dnsOptions,
      name = Some(name),
      useRunc = false,
      parameters ++ containerArgsConfig.extraArgs.map { case (k, v) => ("--" + k, v) })

    // Wrap the container in a CheckpointContainer
    container.map { c =>
      new CheckpointContainer(c, c.containerId, c.addr, getFunctionKey(actionImage), name, this, None)
    }
  }

  /**
   * Adds a checkpoint to the map.
   * This can be called either when creating a new checkpoint or when returning
   * a checkpoint to the available pool after a container is destroyed.
   *
   * @param functionKey The function key
   * @param checkpoint The checkpoint object to add
   */
  def addCheckpoint(functionKey: String, checkpoint: Checkpoint): Unit = {
    val checkpointList = checkpoints.getOrElseUpdate(functionKey, mutable.ListBuffer[Checkpoint]())

    // Add the new checkpoint to the end of the list (most recent)
    checkpointList += checkpoint

    // If we've exceeded the max checkpoints per function, remove the oldest one
    if (checkpointList.size > checkpointConfig.maxCheckpointsPerFunction) {
      val removed = checkpointList.remove(0)
      logging.info(this, s"Removed oldest checkpoint ${removed.checkpointName} for function $functionKey to make room for new checkpoint")
    }

    // Check global checkpoint limit
    val totalCheckpoints = checkpoints.values.map(_.size).sum
    if (totalCheckpoints > checkpointConfig.maxCheckpoints) {
      // Find the function with the oldest checkpoint overall
      val oldestFunction = checkpoints.minBy { case (_, list) =>
        if (list.nonEmpty) list.head.timestamp else Long.MaxValue
      }

      if (oldestFunction._2.nonEmpty) {
        val removed = oldestFunction._2.remove(0)
        logging.info(this, s"Removed oldest checkpoint ${removed.checkpointName} for function ${oldestFunction._1} due to global limit")

        // Clean up empty lists
        if (oldestFunction._2.isEmpty) {
          checkpoints.remove(oldestFunction._1)
        }
      }
    }

    logging.info(this, s"Added checkpoint ${checkpoint.checkpointName} for function $functionKey (total available: ${checkpointList.size})")
  }

  /**
   * Cleans up all containers and checkpoints.
   */
  override def cleanup(): Unit = {
    implicit val transid = TransactionId.invokerNanny

    logging.info(this, "Cleaning up all containers and checkpoints")

    // Remove all containers
    docker
      .ps(filters = Seq("name" -> s"${ContainerFactory.containerNamePrefix(instance)}_"), all = true)
      .foreach { containers =>
        containers.foreach { containerId =>
          docker.rm(containerId).recover {
            case e => logging.error(this, s"Failed to remove container $containerId: ${e.getMessage}")
          }
        }
      }

    // Clear the checkpoints map
    checkpoints.clear()
  }
}