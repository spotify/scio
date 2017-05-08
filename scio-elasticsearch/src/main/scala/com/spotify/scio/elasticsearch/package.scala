package com.spotify.scio

import java.net.InetSocketAddress
import java.time.Duration

import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.SerializableFunction
import org.elasticsearch.action.index.IndexRequest

import scala.concurrent.Future

/**
  * Main package for Elasticsearch APIs. Import all.
  *
  * {{{
  * import com.spotify.scio.elasticsearch._
  * }}}
  */
package object elasticsearch {
  case class ElasticsearchOptions(clusterName: String, servers: Array[InetSocketAddress])

  implicit class ElasticsearchSCollection[T](val self: SCollection[T])
    extends AnyVal {
    /**
      * Save this SCollection into Elasticsearch.
      * Note that the elements must be of a type 'IndexRequestWrapper'
      * @param elasticsearchOptions provides clusterName and server endpoints
      */
    def saveAsElasticsearch(elasticsearchOptions: ElasticsearchOptions,
                            flushInterval: Duration = Duration.ofSeconds(1),
                            f: T => IndexRequest) :Future[Tap[T]] = {
        self.saveAsCustomOutput("Write to Elasticsearch",
          ElasticsearchIO.Write
            .withClusterName(elasticsearchOptions.clusterName)
            .withServers(elasticsearchOptions.servers)
            .withFunction(new SerializableFunction[T, IndexRequest](){
                override def apply(t: T): IndexRequest = f(t)
              }))
    }
  }
}

