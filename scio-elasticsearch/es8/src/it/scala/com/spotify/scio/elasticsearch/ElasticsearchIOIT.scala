package com.spotify.scio.elasticsearch

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.elasticsearch.core.bulk.{BulkOperation, IndexOperation}
import co.elastic.clients.elasticsearch.indices.ExistsRequest
import co.elastic.clients.json.JsonpMapper
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import com.dimafeng.testcontainers.{ElasticsearchContainer, ForAllTestContainer}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.client.RestClient
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Second, Seconds, Span}
import org.testcontainers.utility.DockerImageName

import java.util.UUID
import scala.util.chaining._

object ElasticsearchIOIT {

  val Username = "elastic"
  val Password = "scio"

  final case class Person(
    name: String,
    lastname: String,
    job_description: String
  )

  // Use Jackson for custom types, with scala case class support
  def createScalaMapper(): JsonpMapper =
    new JacksonJsonpMapper().tap(_.objectMapper().registerModule(DefaultScalaModule))
}

class ElasticsearchIOIT extends PipelineSpec with Eventually with ForAllTestContainer {

  import ElasticsearchIOIT._

  override val container = ElasticsearchContainer(
    DockerImageName.parse(s"${ElasticsearchContainer.defaultImage}:8.1.2")
  ).configure(
    _.withEnv("discovery.type", "single-node") // not a cluster
      .withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g") // limit memory for testing
      .withPassword(Password)
  )

  lazy val client: ElasticsearchClient = {
    val credentials = new UsernamePasswordCredentials(Username, Password)
    val provider = new BasicCredentialsProvider()
    provider.setCredentials(AuthScope.ANY, credentials)
    val restClient = RestClient
      .builder(new HttpHost(container.host, container.mappedPort(9200)))
      .setHttpClientConfigCallback(_.setDefaultCredentialsProvider(provider))
      .build()
    val transport = new RestClientTransport(restClient, createScalaMapper())
    new ElasticsearchClient(transport)
  }

  // from https://www.elastic.co/blog/a-practical-introduction-to-elasticsearch
  "ElasticsearchIO" should "apply operations to elasticsearch cluster" in {
    val options = PipelineOptionsFactory.create()
    options.setRunner(classOf[DirectRunner])

    val host = new HttpHost(container.host, container.mappedPort(9200))
    val esOptions = ElasticsearchOptions(
      nodes = Seq(host),
      usernameAndPassword = Some((Username, Password)),
      mapperFactory = createScalaMapper
    )

    val persons = Seq(
      Person("John", "Doe", "Systems administrator and Linux specialist"),
      Person("John", "Smith", "Systems administrator")
    )

    runWithRealContext(options) { sc =>
      sc.parallelize(persons)
        .saveAsElasticsearch(esOptions) { person =>
          val id = UUID.randomUUID()
          val index = IndexOperation.of[Person](
            _.index("accounts").id(id.toString).document(person)
          )
          Some(BulkOperation.of(_.index(index)))
        }
    }

    // give some time to es for indexing
    eventually(timeout(Span(5, Seconds)), interval(Span(1, Second))) {
      client.indices().exists(ExistsRequest.of(_.index("accounts"))).value() shouldBe true
      client.search(SearchRequest.of(_.q("john")), classOf[Person]).hits().total().value() shouldBe 2
    }
  }

}
