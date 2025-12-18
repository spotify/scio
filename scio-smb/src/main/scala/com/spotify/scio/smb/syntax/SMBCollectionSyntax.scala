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

package com.spotify.scio.smb.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder
import com.spotify.scio.smb.SMBCollection
import org.apache.beam.sdk.extensions.smb.{SortedBucketIO, TargetParallelism}

/**
 * Convenience methods for SMBCollection.
 *
 * These are thin wrappers around cogroup that flatten the iterables. Import
 * com.spotify.scio.smb.syntax.all._ to use these methods.
 */
trait SMBCollectionSyntax {

  /**
   * Inner join - cartesian product of matching key groups. Returns (K, (L, R)) for each pair where
   * key matches.
   *
   * Implementation: cogroup + flatMap with nested loop.
   */
  @experimental
  def smbJoin[K: Coder, L: Coder, R: Coder](
    keyClass: Class[K],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit sc: ScioContext): SMBCollection[K, Void, (K, (L, R))] = {
    SMBCollection
      .cogroup2(keyClass, lhs, rhs, targetParallelism)
      .flatMap { case (key, (lIter, rIter)) =>
        for {
          l <- lIter
          r <- rIter
        } yield (key, (l, r))
      }
  }

  /**
   * Left outer join - all records from lhs, with optional matches from rhs. Returns (K, (L,
   * Option[R])).
   *
   * If no match in rhs, returns (l, None) for each l. If matches exist in rhs, returns (l, Some(r))
   * for each pair.
   */
  @experimental
  def smbLeftJoin[K: Coder, L: Coder, R: Coder](
    keyClass: Class[K],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit sc: ScioContext): SMBCollection[K, Void, (K, (L, Option[R]))] = {
    SMBCollection
      .cogroup2(keyClass, lhs, rhs, targetParallelism)
      .flatMap { case (key, (lIter, rIter)) =>
        val rSeq = rIter.toSeq
        if (rSeq.isEmpty) {
          lIter.map(l => (key, (l, None)))
        } else {
          for {
            l <- lIter
            r <- rSeq
          } yield (key, (l, Some(r)))
        }
      }
  }

  /**
   * Right outer join - all records from rhs, with optional matches from lhs. Returns (K,
   * (Option[L], R)).
   */
  @experimental
  def smbRightJoin[K: Coder, L: Coder, R: Coder](
    keyClass: Class[K],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit sc: ScioContext): SMBCollection[K, Void, (K, (Option[L], R))] = {
    SMBCollection
      .cogroup2(keyClass, lhs, rhs, targetParallelism)
      .flatMap { case (key, (lIter, rIter)) =>
        val lSeq = lIter.toSeq
        if (lSeq.isEmpty) {
          rIter.map(r => (key, (None, r)))
        } else {
          for {
            l <- lSeq
            r <- rIter
          } yield (key, (Some(l), r))
        }
      }
  }

  /**
   * Full outer join - all records from both sides, with optional matches. Returns (K, (Option[L],
   * Option[R])).
   *
   * Note: If both sides are empty for a key, no output is produced.
   */
  @experimental
  def smbOuterJoin[K: Coder, L: Coder, R: Coder](
    keyClass: Class[K],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit sc: ScioContext): SMBCollection[K, Void, (K, (Option[L], Option[R]))] = {
    SMBCollection
      .cogroup2(keyClass, lhs, rhs, targetParallelism)
      .flatMap { case (key, (lIter, rIter)) =>
        val (lSeq, rSeq) = (lIter.toSeq, rIter.toSeq)
        (lSeq.isEmpty, rSeq.isEmpty) match {
          case (true, true)   => Iterator.empty // Both empty - no output
          case (true, false)  => rSeq.iterator.map(r => (key, (None, Some(r))))
          case (false, true)  => lSeq.iterator.map(l => (key, (Some(l), None)))
          case (false, false) =>
            for {
              l <- lSeq
              r <- rSeq
            } yield (key, (Some(l), Some(r)))
        }
      }
  }

  /**
   * Implicit class providing pair operations on SMBCollection[K1, K2, (K, V)].
   *
   * Mirrors SCollection's pair operations for consistency.
   */
  @experimental
  implicit class PairSMBCollectionOps[K1, K2, K, V](
    private val self: SMBCollection[K1, K2, (K, V)]
  ) {

    /** Extract just the values, dropping keys. */
    def values(implicit coder: Coder[V]): SMBCollection[K1, K2, V] =
      self.map(_._2)

    /** Extract just the keys, dropping values. */
    def keys(implicit coder: Coder[K]): SMBCollection[K1, K2, K] =
      self.map(_._1)

    /** Transform values while preserving keys. */
    def mapValues[W](f: V => W)(implicit
      keyCoder: Coder[K],
      valueCoder: Coder[W]
    ): SMBCollection[K1, K2, (K, W)] =
      self.map { case (k, v) => (k, f(v)) }

    /** FlatMap over values while preserving keys. */
    def flatMapValues[W](f: V => TraversableOnce[W])(implicit
      keyCoder: Coder[K],
      valueCoder: Coder[W]
    ): SMBCollection[K1, K2, (K, W)] =
      self.flatMap { case (k, v) => f(v).map(w => (k, w)) }

    /** Filter pairs by value predicate. */
    def filterValues(f: V => Boolean)(implicit
      keyCoderK: Coder[K],
      valueCoderV: Coder[V]
    ): SMBCollection[K1, K2, (K, V)] =
      self.filter { case (_, v) => f(v) }

    /** Filter pairs by key predicate. */
    def filterKeys(f: K => Boolean)(implicit
      keyCoderK: Coder[K],
      valueCoderV: Coder[V]
    ): SMBCollection[K1, K2, (K, V)] =
      self.filter { case (k, _) => f(k) }
  }
}

object SMBCollectionSyntax extends SMBCollectionSyntax

/** Import this to get all SMB convenience methods. */
object all extends SMBCollectionSyntax
