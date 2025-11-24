package org.apache.openwhisk.core.pcpm

import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.util.ByteString
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.containerpool.{Container, ContainerAddress, ContainerId}
import org.apache.openwhisk.core.containerpool.docker.{DockerApiWithFileAccess, DockerContainer}
import org.apache.openwhisk.core.entity.ByteSize

import scala.concurrent.{ExecutionContext, Future}

class PCPMContainer(val container: DockerContainer,
                    protected val id: ContainerId,
                    protected[core] val addr: ContainerAddress,
                    protected val pauseContainerName: String,
                    val factory: PauseContainerFactory)(implicit docker: DockerApiWithFileAccess,
                                                        override protected val as: ActorSystem,
                                                        protected val ec: ExecutionContext,
                                                        protected val logging: Logging)
    extends Container {

  /** Obtains logs up to a given threshold from the container. Optionally waits for a sentinel to appear. */
  override def logs(limit: ByteSize, waitForSentinel: Boolean)(
    implicit transid: TransactionId): Source[ByteString, Any] = ???

  // Override destroy to detach from the pause container and return it to the pool
  override def destroy()(implicit transid: TransactionId): Future[Unit] = {
    container.destroy().map { _ =>
      // Return the pause container to the pool
      factory.returnPauseContainer(pauseContainerName)
    }
  }

}
