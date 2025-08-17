package org.apache.openwhisk.core.pcpm

import akka.actor.ActorSystem
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.containerpool.{ContainerFactory, ContainerFactoryProvider}
import org.apache.openwhisk.core.containerpool.docker.{DockerClientWithFileAccess, RuncClient}
import org.apache.openwhisk.core.entity.InvokerInstanceId

object PauseContainerFactoryProvider extends ContainerFactoryProvider {
  override def instance(
                         actorSystem: ActorSystem,
                         logging: Logging,
                         config: WhiskConfig,
                         instanceId: InvokerInstanceId,
                         parameters: Map[String, Set[String]]): ContainerFactory = {

    new PauseContainerFactory(instanceId, parameters)(
      actorSystem,
      actorSystem.dispatcher,
      logging,
      new DockerClientWithFileAccess()(actorSystem.dispatcher)(logging, actorSystem),
      new RuncClient()(actorSystem.dispatcher)(logging, actorSystem))
  }
}
