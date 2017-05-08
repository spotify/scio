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
      *
      * @param elasticsearchOptions defines clusterName and cluster endpoints
      * @param flushInterval delays writes to Elasticsearch cluster to rate limit
      * @param f transforms arbitrary type T to the object required by Elasticsearch client
      * @param numOfShard number of parallel writes to be performed.
      *                   Note: Recommended to be equal to number of workers in your pipeline.
      */
    def saveAsElasticsearch(elasticsearchOptions: ElasticsearchOptions,
                            flushInterval: Duration = Duration.ofSeconds(1),
                            f: T => IndexRequest,
                            numOfShard: Long) :Future[Tap[T]] = {
        self.saveAsCustomOutput("Write to Elasticsearch",
          ElasticsearchIO.Write
            .withClusterName(elasticsearchOptions.clusterName)
            .withServers(elasticsearchOptions.servers)
            .withNumOfShard(numOfShard)
            .withFunction(new SerializableFunction[T, IndexRequest](){
                override def apply(t: T): IndexRequest = f(t)
              }))
    }
  }
}

