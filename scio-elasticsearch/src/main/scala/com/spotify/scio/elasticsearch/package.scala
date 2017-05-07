package com.spotify.scio

import java.net.InetSocketAddress
import java.time.Duration

import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection

import scala.concurrent.Future

/**
  * Main package for Elasticsearch APIs. Import all.
  *
  * {{{
  * import com.spotify.scio.elasticsearch._
  * }}}
  */
package object elasticsearch extends AnyVal {
  case class ElasticsearchOptions(clusterName: String, servers: Array[InetSocketAddress])

  implicit class ElasticsearchSCollection(val self: SCollection[IndexRequestWrapper]) {
    /**
      * Save this SCollection into Elasticsearch.
      * Note that the elements must be of a type 'IndexRequestWrapper'
      * @param elasticsearchOptions provides clusterName and server endpoints
      */
    def saveAsElasticsearch(elasticsearchOptions: ElasticsearchOptions,
                            flushInterval: Duration = Duration.ofSeconds(1))
    :Future[Tap[IndexRequestWrapper]] = {
      self.saveAsCustomOutput("Write to Elasticsearch",
        ElasticsearchIO.Write.withClusterName(elasticsearchOptions.clusterName)
        .withServers(elasticsearchOptions.servers))
    }
  }
}

