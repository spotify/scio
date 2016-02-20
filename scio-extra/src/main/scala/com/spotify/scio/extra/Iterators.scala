/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.extra

import scala.collection.mutable

/** Utilities for Scala iterators. */
object Iterators {

  private[extra] def lowerBound(idx: Long, size: Long, offset: Long): Long =
    if (idx < offset) offset - size else ((idx - offset) / size) * size + offset

  private[extra] def upperBound(idx: Long, size: Long, offset: Long): Long =
    if (idx < offset) offset else ((idx - offset) / size + 1) * size + offset

  private class FixedIterator[T](self: Iterator[T],
                                 timestampFn: T => Long,
                                 size: Long,
                                 offset: Long = 0L) extends Iterator[Seq[T]] {
    private val bi = self.buffered
    override def hasNext: Boolean = bi.hasNext
    override def next(): Seq[T] = {
      val buf = mutable.Buffer(bi.next())
      val start = timestampFn(buf.head)
      val end = upperBound(start, size, offset)

      while (bi.hasNext && timestampFn(bi.head) < end) {
        buf.append(bi.next())
      }
      buf
    }
  }

  private class SessionIterator[T](self: Iterator[T],
                                   timestampFn: T => Long,
                                   gapDuration: Long) extends Iterator[Seq[T]] {
    private val bi = self.buffered
    override def hasNext: Boolean = bi.hasNext
    override def next(): Seq[T] = {
      val buf = mutable.Buffer(bi.next())
      var last = timestampFn(buf.head)

      while (bi.hasNext && timestampFn(bi.head) - last < gapDuration) {
        val n = bi.next()
        buf.append(n)
        last = timestampFn(n)
      }
      buf
    }
  }

  private class SlidingIterator[T](self: Iterator[T],
                                   timestampFn: T => Long,
                                   size: Long,
                                   period: Long = 1L,
                                   offset: Long = 0L) extends Iterator[Seq[T]] {
    private val bi = self.buffered
    private val queue = mutable.Queue[T]()
    fill()

    override def hasNext: Boolean = queue.nonEmpty
    override def next(): Seq[T] = {
      val result = Seq(queue: _*)
      val start = timestampFn(queue.head)
      val upper = upperBound(start, period, offset)
      while (queue.nonEmpty && start < upper) {
        queue.dequeue()
      }
      while (bi.hasNext && timestampFn(bi.head) < upper) {
        bi.next()
      }
      fill()
      result
    }

    private def fill(): Unit = {
      if (queue.isEmpty && bi.hasNext) {
        queue.enqueue(bi.next)
      }
      if (queue.nonEmpty) {
        val start = timestampFn(queue.head)
        val end = if (start < offset) {
          upperBound(start, period, offset) + 1
        } else {
          lowerBound(start, period, offset) + size
        }
        while (bi.hasNext && timestampFn(bi.head) < end) {
          queue.enqueue(bi.next())
        }
      }
    }
  }

  /** Iterator for time series data. */
  class TimeSeriesIterator[T] private[extra] (private val self: Iterator[T], private val timestampFn: T => Long) {

    /**
      * Iterator of fixed-size timestamp-based windows.
      * Partitions the timestamp space into half-open intervals of the form
      * [N * size + offset, (N + 1) * size + offset).
      */
    def fixed(size: Long, offset: Long = 0L): Iterator[Seq[T]] = {
      require(size > 0, "size must be > 0")
      require(offset >= 0, "offset must be >= 0")
      require(offset < size, "offset must be < size")
      new FixedIterator[T](self, timestampFn, size, offset)
    }

    /**
      * Iterator of sessions separated by `gapDuration`-long periods with no elements.
      */
    def session(gapDuration: Long): Iterator[Seq[T]] = {
      require(gapDuration > 0, "size must be > 0")
      new SessionIterator(self, timestampFn, gapDuration)
    }

    /**
      * Iterator of possibly overlapping fixed-size timestamp-based windows.
      * Partitions the timestamp space into half-open intervals of the form
      * [N * period + offset, N * period + offset + size).
      */
    def sliding(size: Long, period: Long = 1L, offset: Long = 0L): Iterator[Seq[T]] = {
      require(size > 0, "size must be > 0")
      require(period > offset, "period must be > offset")
      require(offset >= 0, "offset must be >= 0")
      require(offset < size, "offset must be < size")
      new SlidingIterator[T](self, timestampFn, size, period, offset)
    }

  }

  /** Enhance Iterator by adding a `timeSeries` method. */
  implicit class RichIterator[T](self: Iterator[T]) {
    /**
      * Convert this iterator to a [[TimeSeriesIterator]].
      * @param timestampFn function to extract timestamp.
      */
    def timeSeries(timestampFn: T => Long): TimeSeriesIterator[T] = new TimeSeriesIterator(self, timestampFn)
  }

}
