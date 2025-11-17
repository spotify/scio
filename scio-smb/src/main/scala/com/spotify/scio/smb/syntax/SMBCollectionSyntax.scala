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
import com.spotify.scio.smb.{SMBCollection, SMBKey}
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
  def smbJoin[K: Coder: scala.reflect.ClassTag, L: Coder, R: Coder](
    @annotation.nowarn("cat=unused-params") keyClass: Class[K],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit sc: ScioContext): SMBCollection[K, Void, (L, R)] = {
    SMBCollection
      .cogroup2(SMBKey.primary[K], lhs, rhs, targetParallelism)
      .flatMapValues { case (lIter, rIter) =>
        for {
          l <- lIter
          r <- rIter
        } yield (l, r)
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
  def smbLeftJoin[K: Coder: scala.reflect.ClassTag, L: Coder, R: Coder](
    @annotation.nowarn("cat=unused-params") keyClass: Class[K],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit sc: ScioContext): SMBCollection[K, Void, (L, Option[R])] = {
    SMBCollection
      .cogroup2(SMBKey.primary[K], lhs, rhs, targetParallelism)
      .flatMapValues { case (lIter, rIter) =>
        val rSeq = rIter.toSeq
        if (rSeq.isEmpty) {
          lIter.map(l => (l, None))
        } else {
          for {
            l <- lIter
            r <- rSeq
          } yield (l, Some(r))
        }
      }
  }

  /**
   * Right outer join - all records from rhs, with optional matches from lhs. Returns (K,
   * (Option[L], R)).
   */
  @experimental
  def smbRightJoin[K: Coder: scala.reflect.ClassTag, L: Coder, R: Coder](
    @annotation.nowarn("cat=unused-params") keyClass: Class[K],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit sc: ScioContext): SMBCollection[K, Void, (Option[L], R)] = {
    SMBCollection
      .cogroup2(SMBKey.primary[K], lhs, rhs, targetParallelism)
      .flatMapValues { case (lIter, rIter) =>
        val lSeq = lIter.toSeq
        if (lSeq.isEmpty) {
          rIter.map(r => (None, r))
        } else {
          for {
            l <- lSeq
            r <- rIter
          } yield (Some(l), r)
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
  def smbOuterJoin[K: Coder: scala.reflect.ClassTag, L: Coder, R: Coder](
    @annotation.nowarn("cat=unused-params") keyClass: Class[K],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R],
    targetParallelism: TargetParallelism = TargetParallelism.auto()
  )(implicit sc: ScioContext): SMBCollection[K, Void, (Option[L], Option[R])] = {
    SMBCollection
      .cogroup2(SMBKey.primary[K], lhs, rhs, targetParallelism)
      .flatMapValues { case (lIter, rIter) =>
        val (lSeq, rSeq) = (lIter.toSeq, rIter.toSeq)
        (lSeq.isEmpty, rSeq.isEmpty) match {
          case (true, true)   => Iterator.empty // Both empty - no output
          case (true, false)  => rSeq.iterator.map(r => (None, Some(r)))
          case (false, true)  => lSeq.iterator.map(l => (Some(l), None))
          case (false, false) =>
            for {
              l <- lSeq
              r <- rSeq
            } yield (Some(l), Some(r))
        }
      }
  }
}

object SMBCollectionSyntax extends SMBCollectionSyntax

/** Import this to get all SMB convenience methods. */
object all extends SMBCollectionSyntax
