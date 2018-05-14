package com.spotify.scio

import com.google.cloud.spanner.Mutation
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.spanner.{SpannerConfig, SpannerIO}

import scala.concurrent.Future

/**
  * Main package for Spanner APIs. Import all.
  *
  * {{{
  * import com.spotify.scio.spanner._
  * }}}
  */
package object spanner {

  /** Enhanced version of [[SCollection]] with Spanner methods. */
  implicit class SpannerSCollection(val self: SCollection[Mutation]) extends AnyVal {

    /** Commit [[Mutation]]s to Spanner. */
    def saveAsSpanner(projectId: String,
                      instanceId: String,
                      databaseId: String,
                      batchSizeBytes: Long = 0): Future[Tap[Mutation]] = {

      val spannerConfig = SpannerConfig.create
        .withProjectId(projectId)
        .withInstanceId(instanceId)
        .withDatabaseId(databaseId)

      saveAsSpannerWithConfig(spannerConfig, batchSizeBytes)
    }

    /** Commit [[Mutation]]s to Spanner. */
    def saveAsSpannerWithConfig(spannerConfig: SpannerConfig,
                                batchSizeBytes: Long = 0): Future[Tap[Mutation]] = {

      var sink = SpannerIO.write.withSpannerConfig(spannerConfig)
      if (batchSizeBytes > 0) { sink = sink.withBatchSizeBytes(batchSizeBytes) }

      self.applyInternal(sink)
      Future.failed(new NotImplementedError("Spanner future not implemented."))
    }
  }
}
