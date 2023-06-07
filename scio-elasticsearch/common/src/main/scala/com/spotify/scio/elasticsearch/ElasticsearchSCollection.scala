package com.spotify.scio.elasticsearch

import co.elastic.clients.elasticsearch.core.bulk.BulkOperation
import com.spotify.scio.elasticsearch.ElasticsearchIO.{RetryConfig, WriteParam}
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.elasticsearch.ElasticsearchIO.Write.BulkExecutionException
import org.joda.time.Duration

class ElasticsearchSCollection[T](@transient private val self: SCollection[T]) extends AnyVal {

  /**
   * Save this SCollection into Elasticsearch.
   *
   * @param esOptions
   *   Elasticsearch options
   * @param flushInterval
   *   delays to Elasticsearch writes for rate limiting purpose
   * @param f
   *   function to transform arbitrary type T to Elasticsearch `DocWriteRequest`
   * @param numOfShards
   *   number of parallel writes to be performed, recommended setting is the number of pipeline
   *   workers
   * @param errorFn
   *   function to handle error when performing Elasticsearch bulk writes
   */
  def saveAsElasticsearch(
    esOptions: ElasticsearchOptions,
    flushInterval: Duration = WriteParam.DefaultFlushInterval,
    numOfShards: Long = WriteParam.DefaultNumShards,
    maxBulkRequestOperations: Int = WriteParam.DefaultMaxBulkRequestOperations,
    maxBulkRequestBytes: Long = WriteParam.DefaultMaxBulkRequestBytes,
    errorFn: BulkExecutionException => Unit = WriteParam.DefaultErrorFn,
    retry: RetryConfig = WriteParam.DefaultRetryConfig
  )(f: T => Iterable[BulkOperation]): ClosedTap[Nothing] = {
    val param = WriteParam(
      f,
      errorFn,
      flushInterval,
      numOfShards,
      maxBulkRequestOperations,
      maxBulkRequestBytes,
      retry
    )
    self.write(ElasticsearchIO[T](esOptions))(param)
  }
}
