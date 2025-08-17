package org.apache.openwhisk.core.pcpm

import akka.actor.ActorSystem
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.core.containerpool.docker.{DockerApiWithFileAccess, DockerContainer, RuncApi}
import org.apache.openwhisk.core.containerpool.{Container, ContainerArgsConfig, ContainerFactory, RuntimesRegistryConfig}
import org.apache.openwhisk.core.entity.{ByteSize, ExecManifest, InvokerInstanceId}
import pureconfig.loadConfigOrThrow
import pureconfig._
import pureconfig.generic.auto._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.concurrent.{Await, ExecutionContext, Future}

case class PCPMConfig(poolSize: Int = 5,
                      networkNamespace: String = "bridge",
                      pauseContainerImage: String = "gcr.io/google_containers/pause-amd64:3.1",
                      useRunc: Boolean = true)

class PauseContainerFactory(
  instance: InvokerInstanceId,
  parameters: Map[String, Set[String]],
  containerArgsConfig: ContainerArgsConfig = loadConfigOrThrow[ContainerArgsConfig](ConfigKeys.containerArgs),
  pauseConfig: PCPMConfig = loadConfigOrThrow[PCPMConfig]("whisk.container-pool.pause-container"),
  protected val runtimesRegistryConfig: RuntimesRegistryConfig =
    loadConfigOrThrow[RuntimesRegistryConfig](ConfigKeys.runtimesRegistry),
  protected val userImagesRegistryConfig: RuntimesRegistryConfig =
    loadConfigOrThrow[RuntimesRegistryConfig](ConfigKeys.userImagesRegistry))(implicit actorSystem: ActorSystem,
                                                                              ec: ExecutionContext,
                                                                              logging: Logging,
                                                                              docker: DockerApiWithFileAccess,
                                                                              runc: RuncApi)
    extends ContainerFactory {

  private val pauseContainerPool = ListBuffer[String]()

  override def init(): Unit = {
    cleanup()

    createPauseContainerPool()
  }

  private def createPauseContainerPool(): Unit = {
    implicit val transid = TransactionId.invokerNanny
    implicit val ec: ExecutionContext = actorSystem.dispatcher

    logging.info(this, s"Creating pool of ${pauseConfig.poolSize} pause containers")

    val futures = (1 to pauseConfig.poolSize).map { i =>
      val containerName = s"${ContainerFactory.containerNamePrefix(instance)}_pause_$i"
      val args = Seq("--name", containerName, "--ipc", "shareable")

      docker
        .run(pauseConfig.pauseContainerImage, args)
        .map { containerId =>
            logging.info(this, s"Created pause container: $containerName with ID: $containerId and name: $containerName")
            pauseContainerPool.append(containerName)
        }
        .recover {
          case e =>
            logging.error(this, s"Failed to create pause container: ${e.getMessage}")
        }
    }

    // Wait for all futures to complete before proceeding
    Await.result(Future.sequence(futures), 2.minutes)

    logging.info(this, s"Pause container pool created with ${pauseContainerPool.size} entries")
  }

  // Get an available pause container from the pool
  private def getAvailablePauseContainer(): Option[String] = {
    pauseContainerPool.headOption match {
      case Some(containerName) =>
        pauseContainerPool -= containerName
        Some(containerName)
      case None => None
    }
  }

  // Return a pause container to the pool
  def returnPauseContainer(containerName: String): Unit = {
    pauseContainerPool.append(containerName)
  }

  // Create a container using a pause container's network namespace
  override def createContainer(
    tid: TransactionId,
    name: String,
    actionImage: ExecManifest.ImageName,
    userProvidedImage: Boolean,
    memory: ByteSize,
    cpuShares: Int,
    cpuLimit: Option[Double])(implicit config: WhiskConfig, logging: Logging): Future[Container] = {

    // Get an available pause container
    getAvailablePauseContainer() match {
      case Some(containerName) =>
        val registryConfig =
          ContainerFactory.resolveRegistryConfig(userProvidedImage, runtimesRegistryConfig, userImagesRegistryConfig)
        val image = if (userProvidedImage) Left(actionImage) else Right(actionImage)

        logging.info(this, "Created action container with pause container network: " + s"Container name: $containerName, IP: $containerName" + " container pool size:" + pauseContainerPool.size)

        // Create the container with the network namespace of the pause container
        val container: Future[DockerContainer] = DockerContainer.create(
          tid,
          image = image,
          registryConfig = Some(registryConfig),
          memory = memory,
          cpuShares = cpuShares,
          cpuLimit = cpuLimit,
          environment = Map("__OW_API_HOST" -> config.wskApiHost) ++ containerArgsConfig.extraEnvVarMap,
          network = s"container:$containerName", // We'll use the pause container's network
          dnsServers = Seq.empty, // DNS is handled by the pause container
          dnsSearch = Seq.empty,
          dnsOptions = Seq.empty,
          name = Some(s"${name}_$containerName"),
          useRunc = pauseConfig.useRunc,
          dockerRunParameters =
            parameters ++ containerArgsConfig.extraArgs.map { case (k, v) => ("--" + k, v) } ++ Map(
              "--ipc" -> Set(s"container:$containerName"),
              "--pid" -> Set(s"container:$containerName")
            )
        )

        container.map { c =>
          new PCPMContainer(c, c.containerId, c.addr, containerName, this)
        }

      case None =>
        // If no pause containers are available, fall back to regular container creation
        logging.warn(this, "No pause containers available, falling back to regular container creation")
        val registryConfig =
          ContainerFactory.resolveRegistryConfig(userProvidedImage, runtimesRegistryConfig, userImagesRegistryConfig)
        val image = if (userProvidedImage) Left(actionImage) else Right(actionImage)

        DockerContainer.create(
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
          useRunc = pauseConfig.useRunc,
          parameters ++ containerArgsConfig.extraArgs.map { case (k, v) => ("--" + k, v) })
    }
  }

  // Clean up all pause containers
  override def cleanup(): Unit = {
    implicit val transid = TransactionId.invokerNanny

    logging.info(this, "Cleaning up all containers")

    docker
      .ps(filters = Seq("name" -> s"${ContainerFactory.containerNamePrefix(instance)}_"), all = true)
      .foreach { containers =>
        containers.foreach { containerId =>
          // Try to unpause the container if it's paused
          (if (pauseConfig.useRunc) {
            runc.resume(containerId)
          } else {
            docker.unpause(containerId)
          })
            .recoverWith {
              // Ignore resume failures and try to remove anyway
              case _ => Future.successful(())
            }
            .flatMap { _ =>
              docker.rm(containerId).recover {
                case e => logging.error(this, s"Failed to remove action container $containerId: ${e.getMessage}")
              }
            }
        }
      }

    // Then, remove all pause containers
    logging.info(this, "Cleaning up pause containers")

    // Get all pause containers for this instance
    docker
      .ps(filters = Seq("name" -> s"${ContainerFactory.containerNamePrefix(instance)}_pause_"), all = true)
      .foreach { containers =>
        containers.foreach { containerId =>
          docker.rm(containerId).recover {
            case e => logging.error(this, s"Failed to remove pause container $containerId: ${e.getMessage}")
          }
        }
      }

    pauseContainerPool.clear()
  }
}
