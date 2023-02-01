package com.spotify.scio.cosmosdb

import com.dimafeng.testcontainers.SingleContainer
import com.spotify.scio.cosmosdb.ScalaCosmosDBEmulatorContainer.defaultDockerImageName
import org.testcontainers.containers.CosmosDBEmulatorContainer
import org.testcontainers.utility.DockerImageName

import java.security.KeyStore

case class ScalaCosmosDBEmulatorContainer(
  dockerImageName: DockerImageName = DockerImageName.parse(defaultDockerImageName)
) extends SingleContainer[CosmosDBEmulatorContainer] {

  override val container: CosmosDBEmulatorContainer = new CosmosDBEmulatorContainer(dockerImageName)

  def buildNewKeyStore: KeyStore = container.buildNewKeyStore
  def emulatorEndpoint: String = container.getEmulatorEndpoint
  def emulatorKey: String = container.getEmulatorKey
}

object ScalaCosmosDBEmulatorContainer {
  val defaultImage = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator"
  val defaultTag = "latest"
  val defaultDockerImageName = s"$defaultImage:$defaultTag"
}
