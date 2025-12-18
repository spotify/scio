/*
 * Copyright 2025 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.smb

import org.apache.beam.sdk.extensions.smb.{SortedBucketIO, SortedBucketSource}
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.values.KV

/**
 * Simplified key-group reader for SMB sources.
 *
 * Merges sorted key groups from multiple sources using linear scan across sources. Internally uses
 * lazy iterators wrapped in ExhaustableLazyIterable for memory efficiency, but materializes to Seq
 * at API boundary to maintain backward compatibility.
 *
 * Returns List[Seq[Any]] where index i corresponds to source i, eliminating the need for
 * CoGbkResult and TupleTag lookups.
 *
 * @param sources
 *   List of bucketed inputs (typically 2-3 sources for joins)
 * @param keyFn
 *   Function to convert ComparableKeyBytes to user key type K
 * @param keyComparator
 *   Comparator for ordering keys
 * @param metricsPrefix
 *   Prefix for metrics (e.g., "SMBCollection")
 * @param bucketId
 *   Bucket ID being processed
 * @param effectiveParallelism
 *   Effective parallelism for this transform
 * @param options
 *   Pipeline options
 */
private[smb] class SimpleKeyGroupReader[K](
  sources: List[SortedBucketSource.BucketedInput[_]],
  keyFn: SortedBucketIO.ComparableKeyBytes => K,
  keyComparator: java.util.Comparator[SortedBucketIO.ComparableKeyBytes],
  @annotation.nowarn("cat=unused-params") metricsPrefix: String,
  bucketId: Int,
  effectiveParallelism: Int,
  options: PipelineOptions
) {

  // Metric disabled in Phase 2 - can't track size without materialization
  // private val keyGroupSize: Distribution =
  //   Metrics.distribution(metricsPrefix, s"$metricsPrefix-KeyGroupSize")

  // Fixed-size array of source iterators (typically 2-3 sources)
  private val sourceIters: Array[SourceIterator] =
    sources.map(src => new SourceIterator(src, bucketId, effectiveParallelism, options)).toArray

  private val numSources = sourceIters.length

  /**
   * Read next key group, returning List[Iterable[Any]] where index corresponds to source index.
   * Returns null when all sources exhausted.
   *
   * Phase 2: Returns lazy ExhaustableLazyIterables for memory efficiency.
   */
  def readNext(): KV[K, List[Iterable[Any]]] = {
    // Find minimum key across all active sources (linear scan - optimal for small n)
    var minKey: SortedBucketIO.ComparableKeyBytes = null
    var i = 0
    while (i < numSources) {
      val iter = sourceIters(i)
      if (iter.hasNext) {
        val key = iter.peekKey()
        if (minKey == null || keyComparator.compare(key, minKey) < 0) {
          minKey = key
        }
      }
      i += 1
    }

    // All sources exhausted
    if (minKey == null) {
      return null
    }

    // Create lazy iterables for each source
    val lazyIterables = Array.fill[ExhaustableLazyIterable[Any]](numSources)(
      new ExhaustableLazyIterable(Iterator.empty)
    )

    // Consume minKey from all sources that have it
    i = 0
    while (i < numSources) {
      val iter = sourceIters(i)
      if (iter.hasNext && keyComparator.compare(iter.peekKey(), minKey) == 0) {
        val valueIterator = iter.consumeCurrentKeyGroupAsIterator()
        lazyIterables(i) = new ExhaustableLazyIterable(valueIterator)
      }
      i += 1
    }

    // Phase 2: Return lazy iterables directly (no materialization)
    // Upcast to Iterable for API compatibility
    val valuesBySource: Array[Iterable[Any]] = lazyIterables.map(x => x: Iterable[Any])

    // Note: Can't track keyGroupSize with lazy iteration (would require materialization)
    // Metric is now disabled - can be re-enabled if needed by counting during iteration
    // keyGroupSize.update(totalValues)  // DISABLED

    KV.of(keyFn(minKey), valuesBySource.toList)
  }

  /** Iterator over a single source's key groups. Wraps the underlying KeyGroupIterator. */
  private class SourceIterator(
    source: SortedBucketSource.BucketedInput[_],
    bucketId: Int,
    parallelism: Int,
    options: PipelineOptions
  ) {
    // Use Java Iterator interface since KeyGroupIterator is package-private
    private val iter
      : java.util.Iterator[KV[SortedBucketIO.ComparableKeyBytes, java.util.Iterator[_]]] =
      source
        .createIterator(bucketId, parallelism, options)
        .asInstanceOf[java.util.Iterator[
          KV[SortedBucketIO.ComparableKeyBytes, java.util.Iterator[_]]
        ]]

    // Use existential type to avoid variance issues with KV
    private var head: KV[SortedBucketIO.ComparableKeyBytes, _ <: java.util.Iterator[_]] =
      if (iter.hasNext) iter.next() else null

    def hasNext: Boolean = head != null

    def peekKey(): SortedBucketIO.ComparableKeyBytes = head.getKey

    /**
     * Return a lazy iterator over all values for the current key from this source. The iterator
     * auto-advances to the next key group when exhausted.
     *
     * IMPORTANT: The returned iterator MUST be fully consumed before calling peekKey() or
     * consumeCurrentKeyGroupAsIterator() again.
     */
    def consumeCurrentKeyGroupAsIterator(): Iterator[Any] = {
      val valueIter = head.getValue

      // Wrap in auto-advancing iterator
      new Iterator[Any] {
        def hasNext: Boolean = {
          val has = valueIter.hasNext
          if (!has) {
            // Iterator exhausted, advance to next key group
            head = if (iter.hasNext) iter.next() else null
          }
          has
        }

        def next(): Any = {
          if (!hasNext) {
            throw new NoSuchElementException("Iterator exhausted")
          }
          valueIter.next()
        }
      }
    }
  }
}
