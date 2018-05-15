package com.spotify.scio

import com.google.cloud.spanner._
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.spanner.{MutationGroup, SpannerConfig, SpannerIO}

import scala.collection.JavaConverters._
import scala.concurrent.Future

/**
 * Main package for Spanner APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.spanner._
 * }}}
 */
package object spanner {

  /** Enhanced version of [[ScioContext]] with Spanner methods. */
  implicit class SpannerScioContext(val self: ScioContext) extends AnyVal {

    /** Read from Spanner table. Return [[SCollection]] of [[Struct]]s. */
    def spannerFromTable(projectId: String,
                         instanceId: String,
                         databaseId: String,
                         table: String,
                         columns: Iterable[String],
                         keySet: KeySet = null,
                         partitionOptions: PartitionOptions = null,
                         timestampBound: TimestampBound = null): SCollection[Struct] = {

      val spannerConfig = SpannerConfig.create
        .withProjectId(projectId)
        .withInstanceId(instanceId)
        .withDatabaseId(databaseId)

      spannerFromTableWithConfig(
        spannerConfig, table, columns, keySet, partitionOptions, timestampBound)
    }

    /** Read from Spanner table. Return [[SCollection]] of [[Struct]]s. */
    def spannerFromTableWithConfig(spannerConfig: SpannerConfig,
                                   table: String,
                                   columns: Iterable[String],
                                   keySet: KeySet = null,
                                   partitionOptions: PartitionOptions = null,
                                   timestampBound: TimestampBound = null): SCollection[Struct] = {

      var read = SpannerIO.read.withSpannerConfig(spannerConfig)
        .withTable(table)
        .withColumns(columns.toSeq.asJava)

      if (keySet != null) { read = read.withKeySet(keySet) }
      if (partitionOptions != null) { read = read.withPartitionOptions(partitionOptions) }
      if (timestampBound != null) { read = read.withTimestampBound(timestampBound) }

      self.wrap(self.applyInternal(read))
    }

    /** Read from Spanner with query. Return [[SCollection]] of [[Struct]]s. */
    def spannerFromQuery(projectId: String,
                         instanceId: String,
                         databaseId: String,
                         query: String,
                         index: String = null,
                         partitionOptions: PartitionOptions = null,
                         timestampBound: TimestampBound = null): SCollection[Struct] = {

      val spannerConfig = SpannerConfig.create
        .withProjectId(projectId)
        .withInstanceId(instanceId)
        .withDatabaseId(databaseId)

      spannerFromQueryWithConfig(spannerConfig, query, index, partitionOptions, timestampBound)
    }

    /** Read from Spanner with query. Return [[SCollection]] of [[Struct]]s. */
    def spannerFromQueryWithConfig(spannerConfig: SpannerConfig,
                                   query: String,
                                   index: String = null,
                                   partitionOptions: PartitionOptions = null,
                                   timestampBound: TimestampBound = null): SCollection[Struct] = {

      var read = SpannerIO.read.withSpannerConfig(spannerConfig).withQuery(query)

      if (index != null) { read = read.withIndex(index) }
      if (partitionOptions != null) { read = read.withPartitionOptions(partitionOptions) }
      if (timestampBound != null) { read = read.withTimestampBound(timestampBound) }

      self.wrap(self.applyInternal(read))
    }
  }

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

      var write = SpannerIO.write.withSpannerConfig(spannerConfig)
      if (batchSizeBytes > 0) { write = write.withBatchSizeBytes(batchSizeBytes) }

      self.applyInternal(write)
      Future.failed(new NotImplementedError("Spanner future not implemented."))
    }
  }

  /**
   * Enhanced version of [[SCollection]] with Spanner methods for committing groups
   * of [[Mutation]] atomically ([[MutationGroup]]).
   */
  implicit class SpannerMutationGroupSCollection(val self: SCollection[MutationGroup])
    extends AnyVal {

    /** Commit [[MutationGroup]]s to Spanner. */
    def saveAsSpanner(projectId: String,
                      instanceId: String,
                      databaseId: String,
                      batchSizeBytes: Long = 0): Future[Tap[MutationGroup]] = {

      val spannerConfig = SpannerConfig.create
        .withProjectId(projectId)
        .withInstanceId(instanceId)
        .withDatabaseId(databaseId)

      saveAsSpannerWithConfig(spannerConfig, batchSizeBytes)
    }

    /** Commit [[MutationGroup]]s to Spanner. */
    def saveAsSpannerWithConfig(spannerConfig: SpannerConfig,
                                batchSizeBytes: Long = 0): Future[Tap[MutationGroup]] = {

      var write = SpannerIO.write.withSpannerConfig(spannerConfig)
      if (batchSizeBytes > 0) { write = write.withBatchSizeBytes(batchSizeBytes) }

      self.applyInternal(write.grouped)
      Future.failed(new NotImplementedError("Spanner future not implemented."))
    }
  }
}
