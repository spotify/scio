/*
 * Copyright 2024 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.smb

/**
 * Single-use Iterable that wraps a lazy Iterator.
 *
 * Guarantees:
 *   - Can only be iterated once (fail-fast on second call)
 *   - Can be exhausted without iteration (for cleanup)
 *   - Clear error message guides users to materialize with .toSeq
 *
 * Usage:
 * {{{
 * val iterable = new ExhaustableLazyIterable(iterator)
 *
 * // Option 1: Single-pass consumption (lazy)
 * iterable.foldLeft(0)(_ + _)  // OK - consumes once
 *
 * // Option 2: Multiple passes (materialize first)
 * val seq = iterable.toSeq
 * val sum = seq.sum
 * val max = seq.max  // OK - Seq is re-iterable
 *
 * // Option 3: Never consumed (auto-exhausted by framework)
 * // iterable is passed to consumer but never used
 * // Framework calls exhaust() to ensure iterator advances
 * }}}
 *
 * @param underlying
 *   The lazy iterator to wrap
 * @tparam T
 *   Element type
 */
private[smb] class ExhaustableLazyIterable[T](
  private val underlying: Iterator[T]
) extends Iterable[T] {

  private var initialized = false
  private var firstHasNext: Boolean = false
  private var allowedIterator: Iterator[T] = null
  private var elementCount: Long = 0L // Track total elements consumed

  /**
   * Lazily initialize the iterator state on first access. Caches `hasNext` value to support
   * isEmpty/nonEmpty without consuming elements.
   */
  private def ensureInitialized(): Unit = {
    if (!initialized) {
      firstHasNext = underlying.hasNext
      initialized = true
    }
  }

  /**
   * Returns a new iterator. All returned iterators can check hasNext (returns cached value), but
   * only the first iterator to call next() is allowed to consume elements.
   */
  override def iterator: Iterator[T] = {
    ensureInitialized()

    new Iterator[T] {
      override def hasNext: Boolean = {
        if (allowedIterator == null) {
          // No iterator has started consuming yet - return cached initial state
          firstHasNext
        } else if (allowedIterator eq this) {
          // This iterator is consuming - return current state
          underlying.hasNext
        } else {
          // Another iterator is consuming - return cached initial state
          // This allows Scala 2.12's headOption to work: first iterator checks isEmpty,
          // second iterator checks hasNext before calling next()
          firstHasNext
        }
      }

      override def next(): T = {
        val elem = if (allowedIterator == null) {
          // This is the first iterator to call next() - allow it
          allowedIterator = this
          underlying.next()
        } else if (allowedIterator eq this) {
          // This iterator already called next() before - continue consuming
          underlying.next()
        } else {
          // A different iterator tried to call next() - this is double consumption!
          throw new IllegalStateException(
            """SMBCollection values are lazy iterables that can only be consumed once.
              |
              |This error typically occurs when you fan out to multiple outputs from the same base collection.
              |
              |Example problem:
              |  val base = SMBCollection.cogroup2(key, lhs, rhs)
              |  base.mapValues { case (lhs, rhs) => lhs.size }.saveAsSortedBucket(out1)
              |  base.mapValues { case (lhs, rhs) => rhs.size }.saveAsSortedBucket(out2)
              |  // ERROR: The same (lhs, rhs) tuple is consumed by both outputs!
              |
              |Fix by materializing at the fanout point:
              |  val materialized = base.mapValues { case (lhs, rhs) => (lhs.toSeq, rhs.toSeq) }
              |  materialized.mapValues { case (lhs, rhs) => lhs.size }.saveAsSortedBucket(out1)
              |  materialized.mapValues { case (lhs, rhs) => rhs.size }.saveAsSortedBucket(out2)
              |
              |For single-output pipelines, no materialization is needed - values flow lazily for maximum efficiency.
              |""".stripMargin
          )
        }
        elementCount += 1
        elem
      }
    }
  }

  /**
   * Exhaust any remaining values in the iterator. Safe to call whether or not iterator() was
   * called.
   *
   * Called automatically by the framework after each key group is processed to ensure the
   * underlying data source can advance to the next key.
   */
  def exhaust(): Unit = {
    // Consume all remaining values
    // If iterator() was called and partially consumed, this finishes it
    // If iterator() was never called, this consumes from the beginning
    while (underlying.hasNext) {
      underlying.next()
    }
  }

  /**
   * Check if the underlying iterator has been started (next() was called on any iterator). Useful
   * for debugging/testing.
   */
  def isStarted: Boolean = allowedIterator != null

  /** Check if the underlying iterator is exhausted. Useful for debugging/testing. */
  def isExhausted: Boolean = !underlying.hasNext

  /** Get the total number of elements consumed from this iterable. */
  def getElementCount: Long = elementCount

  /**
   * Provide meaningful string representation for debugging. Shows current state: pending,
   * in-progress, or exhausted.
   */
  override def toString: String = {
    val state =
      if (!initialized) "pending"
      else if (allowedIterator == null) "initialized (not consumed)"
      else if (isExhausted) "exhausted"
      else "in-progress"
    s"ExhaustableLazyIterable($state)"
  }
}
