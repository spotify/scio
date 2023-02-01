package com.spotify.scio.cosmosdb

import com.azure.cosmos.CosmosClientBuilder
import com.dimafeng.testcontainers.ForAllTestContainer
import com.spotify.scio.cosmosdb.Utils.initLog
import com.spotify.scio.{ ContextAndArgs, ScioMetrics }
import org.bson.Document
import org.junit.rules.TemporaryFolder
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Files
import scala.util.Using

/** sbt scio-cosmosdb/IntegrationTest/test */
class CosmosDbIOIT extends AnyFlatSpec with Matchers with BeforeAndAfterAll with ForAllTestContainer {
  private val DATABASE = "test"
  private val CONTAINER = "test"
  private val tempFolder = new TemporaryFolder
  tempFolder.create()
  initLog()

  override val container: ScalaCosmosDBEmulatorContainer = ScalaCosmosDBEmulatorContainer()

  override def beforeAll(): Unit = {
    val keyStoreFile = tempFolder.newFile("azure-cosmos-emulator.keystore").toPath
    val keyStore = container.buildNewKeyStore
    keyStore.store(
      Files.newOutputStream(keyStoreFile.toFile.toPath),
      container.emulatorKey.toCharArray
    )
    System.setProperty("javax.net.ssl.trustStore", keyStoreFile.toString)
    System.setProperty("javax.net.ssl.trustStorePassword", container.emulatorKey)
    System.setProperty("javax.net.ssl.trustStoreType", "PKCS12")

    scribe.info("Create data -------------------------------------------------------->")
    val triedCreateData = Using(
      new CosmosClientBuilder().gatewayMode
        .endpointDiscoveryEnabled(false)
        .endpoint(container.emulatorEndpoint)
        .key(container.emulatorKey)
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
    scribe.info("Data created ------------------------------------------------------------<")
  }

  behavior of "CosmosDb with Core (SQL) API"

  it should "be " in {
    val output = tempFolder.newFolder("output.txt")
    scribe.info(s"output path: ${output.getPath}")

    val (sc, _) = ContextAndArgs(Array())

    val counter = ScioMetrics.counter("counter")
    sc
      .readCosmosDbCoreApi(
        container.emulatorEndpoint,
        container.emulatorKey,
        DATABASE,
        CONTAINER,
        s"SELECT * FROM c"
      )
      .tap(_ => counter.inc())
      .map(_.toJson)
      .saveAsTextFile(output.getPath)

    val result = sc.run().waitUntilFinish()

    result.counter(counter).committed.get should equal(10)
  }

}




