package com.spotify.scio

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
package object elasticsearch extends AnyVal{
  implicit class ElasticsearchSCollection(val self: SCollection[IndexRequestWrapper]) {
    /**
      * Save this SCollection into Elasticsearch. Default flushInterval is set to 1 secnod.
      * Note that the elements must be of a type 'IndexRequestWrapper'
      * @param elasticsearchOptions provides clusterName and server endpoints
      */
    def saveAsElasticsearch(elasticsearchOptions: ElasticsearchOptions)
    :Future[Tap[IndexRequestWrapper]] = {
      self.saveAsCustomOutput("Write to Elasticsearch",
        ElasticsearchIO.Write.withElasticsearchOptions(elasticsearchOptions))
    }

    /**
      * Save this SCollection into Elasticsearch.
      * Note that the elements must be of a type 'IndexRequestWrapper'
      * @param elasticsearchOptions provides clusterName and server endpoints
      * @param flushInterval delayed applied to rate limit writes to Elasticsearch cluster
      */
    def saveAsElasticsearch(elasticsearchOptions: ElasticsearchOptions,
                            flushInterval: Duration)
    : Future[Tap[IndexRequestWrapper]] = {
      self.saveAsCustomOutput("Write to Elasticsearch",
        ElasticsearchIO.Write
        .withElasticsearchOptions(elasticsearchOptions)
        .withFlushInterval(flushInterval))
    }
  }
}
