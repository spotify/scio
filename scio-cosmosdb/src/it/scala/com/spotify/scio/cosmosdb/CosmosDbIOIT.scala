package com.spotify.scio.cosmosdb

import com.azure.cosmos.CosmosClientBuilder
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.cosmosdb.Utils.initLog
import org.bson.Document
import org.junit.rules.TemporaryFolder
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.testcontainers.containers.CosmosDBEmulatorContainer
import org.testcontainers.utility.DockerImageName

import java.nio.file.Files
import scala.util.Using

/** sbt scio-cosmosdb/IntegrationTest/test */
class CosmosDbIOIT extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val DOCKER_NAME = "mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:latest"
  private val DATABASE = "test"
  private val CONTAINER = "test"
  private val cosmosDBEmulatorContainer = new CosmosDBEmulatorContainer(
    DockerImageName.parse(DOCKER_NAME)
  )
  private val tempFolder = new TemporaryFolder
  tempFolder.create()
  initLog()

  override def beforeAll(): Unit = {
    scribe.info("Star CosmosDB emulator")
    cosmosDBEmulatorContainer.start()

    val keyStoreFile = tempFolder.newFile("azure-cosmos-emulator.keystore").toPath
    val keyStore = cosmosDBEmulatorContainer.buildNewKeyStore
    keyStore.store(
      Files.newOutputStream(keyStoreFile.toFile.toPath),
      cosmosDBEmulatorContainer.getEmulatorKey.toCharArray
    )
    System.setProperty("javax.net.ssl.trustStore", keyStoreFile.toString)
    System.setProperty("javax.net.ssl.trustStorePassword", cosmosDBEmulatorContainer.getEmulatorKey)
    System.setProperty("javax.net.ssl.trustStoreType", "PKCS12")

    scribe.info("Creando la data -------------------------------------------------------->")
    val triedCreateData = Using(
      new CosmosClientBuilder().gatewayMode
        .endpointDiscoveryEnabled(false)
        .endpoint(cosmosDBEmulatorContainer.getEmulatorEndpoint)
        .key(cosmosDBEmulatorContainer.getEmulatorKey)
        .buildClient
    ) { client =>
      client.createDatabase(DATABASE)
      val db = client.getDatabase(DATABASE)
      db.createContainer(CONTAINER, "/id")
      val container = db.getContainer(CONTAINER)
      for (i <- 1 to 10) {
        container.createItem(new Document("id", i.toString))
      }
    }
    if (triedCreateData.isFailure) {
      val throwable = triedCreateData.failed.get
      scribe.error("Error creando la data", throwable)
      throw throwable
    }
    scribe.info("Data creada ------------------------------------------------------------<")
  }

  override protected def afterAll(): Unit = {
    scribe.info("Stop CosmosDB emulator")
    cosmosDBEmulatorContainer.stop()
  }

  behavior of "CosmosDb with Core (SQL) API"

  it should "be " in {
    val output = tempFolder.newFolder("output.txt")
    scribe.info(s"output path: ${output.getPath}")

    val (sc, args) = ContextAndArgs(Array())
    val a = sc
      .readCosmosDbCoreApi(
        cosmosDBEmulatorContainer.getEmulatorEndpoint,
        cosmosDBEmulatorContainer.getEmulatorKey,
        DATABASE,
        CONTAINER,
        s"SELECT * FROM c"
      )
      .map(_.toJson)
      .saveAsTextFile(output.getPath)

    sc.run()
  }

}
