/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.openwhisk.core.containerpool

import akka.actor.ActorSystem
import org.apache.openwhisk.common.Logging
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.containerpool.docker.StandaloneDockerContainerFactoryProvider
import org.apache.openwhisk.core.entity.InvokerInstanceId
import org.apache.openwhisk.spi.Spi
import pureconfig._
import pureconfig.generic.auto._

case class ContainerStrategyConfig(strategy: String)

/**
 * A ContainerFactoryProvider that switches between different container strategies
 * based on a configuration parameter.
 * 
 * Supported strategies:
 *   - "standard" : Uses StandaloneDockerContainerFactoryProvider (default)
 *   - "pause"    : Uses PauseContainerFactoryProvider
 *   - "checkpoint": Uses CheckpointContainerFactoryProvider
 * 
 * Configuration:
 *   whisk.container-factory.strategy = "standard" | "pause" | "checkpoint"
 * 
 * Can also be set via environment variable: CONTAINER_STRATEGY
 */
object SwitchableContainerFactoryProvider extends ContainerFactoryProvider with Spi {
  
  val StandardStrategy = "standard"
  val PauseStrategy = "pause"
  val CheckpointStrategy = "checkpoint"
  
  def instance(
    actorSystem: ActorSystem,
    logging: Logging,
    config: WhiskConfig,
    instance: InvokerInstanceId,
    parameters: Map[String, Set[String]]
  ): ContainerFactory = {
    
    // Read strategy from config, with fallback to environment variable, with default to "standard"
    val strategyConfig = loadConfigOrThrow[ContainerStrategyConfig]("whisk.container-factory")
    val strategy = sys.env.getOrElse("CONTAINER_STRATEGY", strategyConfig.strategy).toLowerCase.trim
    
    logging.info(this, s"Container factory strategy: $strategy")
    
    strategy match {
      case StandardStrategy =>
        logging.info(this, "Using StandaloneDockerContainerFactoryProvider")
        StandaloneDockerContainerFactoryProvider.instance(actorSystem, logging, config, instance, parameters)
        
      case PauseStrategy =>
        logging.info(this, "Using PauseContainerFactoryProvider")
        // Dynamically load to avoid compile-time dependency issues
        val providerClass = Class.forName("org.apache.openwhisk.core.pcpm.PauseContainerFactoryProvider$")
        val provider = providerClass.getField("MODULE$").get(null).asInstanceOf[ContainerFactoryProvider]
        provider.instance(actorSystem, logging, config, instance, parameters)
        
      case CheckpointStrategy =>
        logging.info(this, "Using CheckpointContainerFactoryProvider")
        // Dynamically load to avoid compile-time dependency issues
        val providerClass = Class.forName("org.apache.openwhisk.core.checkpoint.CheckpointContainerFactoryProvider$")
        val provider = providerClass.getField("MODULE$").get(null).asInstanceOf[ContainerFactoryProvider]
        provider.instance(actorSystem, logging, config, instance, parameters)
        
      case unknown =>
        logging.warn(this, s"Unknown container strategy '$unknown', falling back to 'standard'")
        StandaloneDockerContainerFactoryProvider.instance(actorSystem, logging, config, instance, parameters)
    }
  }
}
