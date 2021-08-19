/*
 * Copyright 2021 Spotify AB.
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

package com.spotify.scio.extra.rollup.syntax

import com.spotify.scio.coders.BeamCoders
import com.spotify.scio.values.SCollection
import com.twitter.algebird.Group

trait SCollectionSyntax {

  implicit final class RollupOps[U, D, R, M](self: SCollection[(U, D, R, M)]) {

    /**
     * Takes an [[SCollection]] with elements consisting of three sets of dimensions and one measure
     * and returns an [[SCollection]] tuple, where the key is a set of dimensions and the value the
     * summed measure combined with a distinct count.
     *
     * This is to be used when doing a count distinct for one key over a set of dimensions, when
     * that key can be present in multiple elements in the final dataset, such that there is a need
     * to provide additional rollups over the non-unique dimensions where distinct counts are not
     * summable.
     *
     * U - Unique key, this is what we want to count distinct occurrences of D - Dimensions that
     * should not be rolled up (these are either unique per U or we are not expected to sum U over
     * these dimensions, eg. a metric for different dates) R - Dimensions that should be rolled up M
     * - Additional measure that is summable over all dimensions
     *
     * @param rollupFunction
     *   A function takes one element with dimensions of type R and returns a set of R with one
     *   element for each combination of rollups that we want to provide
     */
    def rollupAndCount(
      rollupFunction: R => Set[R]
    )(implicit g: Group[M]): SCollection[((D, R), (M, Long))] = {
      implicit val (coderU, coderD, coderR, coderM) = BeamCoders.getTuple4Coders(self)

      val doubleCounting = self
        .withName("RollupAndCountDuplicates")
        .transform {
          _.map { case (_, dims, rollupDims, measure) =>
            ((dims, rollupDims), (measure, 1L))
          }.sumByKey
            .flatMap { case (dims @ (_, rollupDims), measure) =>
              rollupFunction(rollupDims)
                .map((x: R) => dims.copy(_2 = x) -> measure)
            }
        }

      val correctingCounts = self
        .withName("RollupAndCountCorrection")
        .transform {
          _.map { case (uniqueKey, dims, rollupDims, _) =>
            ((uniqueKey, dims), rollupDims)
          }.groupByKey
            .filterValues(_.size > 1)
            .flatMapValues { values =>
              val rollupMap = collection.mutable.Map.empty[R, Long]
              for (r <- values) {
                for (newDim <- rollupFunction(r)) {
                  // Add 1 to correction count. We only care to correct for excessive counts
                  rollupMap(newDim) = rollupMap.getOrElse(newDim, 1L) - 1L
                }
              }
              // We only care about correcting cases where we actually double-count
              rollupMap.iterator.filter(_._2 < 0L)
            }
            .map { case ((_, dims), (rollupDims, count)) => ((dims, rollupDims), (g.zero, count)) }
        }

      SCollection
        .unionAll(List(doubleCounting, correctingCounts))
        .withName("RollupAndCountCorrected")
        .sumByKey

    }
  }

}
