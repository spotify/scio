/*
 * Copyright 2024 Spotify AB
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

package com.spotify.scio.values

import com.spotify.scio.util.ScioUtil
import com.spotify.scio.util.random.{BernoulliSampler, PoissonSampler}
import org.apache.beam.sdk.transforms.Sample

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Random

object SampleSCollectionFunctions {

  // weight-based sampling models

  final private case class WeightedSample[T](id: Long, value: T, weight: Long)
  private object WeightedSample {
    implicit def ordering[T]: Ordering[WeightedSample[T]] =
      Ordering.by[WeightedSample[T], Long](_.id).reverse
  }

  final private case class WeightedCombiner[T](
    weight: Long = 0L,
    queue: mutable.PriorityQueue[WeightedSample[T]] = mutable.PriorityQueue.empty[WeightedSample[T]]
  )
}

class SampleSCollectionFunctions[T](self: SCollection[T]) {
  import SampleSCollectionFunctions._

  /**
   * Return a sampled subset of this SCollection containing exactly `sampleSize` items. Involves
   * combine operation resulting in shuffling. All the elements of the output should fit into main
   * memory of a single worker machine.
   *
   * @return
   *   a new SCollection whose single value is an `Iterable` of the samples
   * @group transform
   */
  def sample(sampleSize: Int): SCollection[Iterable[T]] = self.transform {
    import self.coder
    _.pApply(Sample.fixedSizeGlobally(sampleSize)).map(_.asScala)
  }

  /**
   * Return a sampled subset of this SCollection containing weighted items with at most
   * `totalWeight` sum. Involves combine operation resulting in shuffling. All the elements of the
   * output should fit into main memory of a single worker machine.
   *
   * @return
   *   a new SCollection whose single value is an `Iterable` of the samples
   * @group transform
   */
  def sampleWeighted(
    totalWeight: Long,
    cost: T => Long
  ): SCollection[Iterable[T]] = {
    import self.coder
    val mergeValue = { (c: WeightedCombiner[T], x: T) =>
      val sample = WeightedSample(Random.nextLong(), x, cost(x))

      val queue = c.queue
      var weight = c.weight
      // drop all elements with lower priority than the current sample if the queue is full
      while (weight + sample.weight > totalWeight && queue.headOption.exists(_.id < sample.id)) {
        val removed = queue.dequeue()
        weight -= removed.weight
      }
      // add the current sample if there is enough space
      if (weight + sample.weight <= totalWeight) {
        queue += sample
        weight += sample.weight
      }
      WeightedCombiner(weight, queue)
    }

    val mergeCombiners = { (l: WeightedCombiner[T], r: WeightedCombiner[T]) =>
      // merge the two queues, reusing the left one
      val queue = l.queue ++= r.queue
      var weight = l.weight + r.weight
      // drop all elements with low priority until the total weight is less than the limit
      while (weight > totalWeight && queue.nonEmpty) {
        val removed = queue.dequeue()
        weight -= removed.weight
      }

      WeightedCombiner(weight, queue)
    }

    self
      .aggregate(WeightedCombiner[T]())(mergeValue, mergeCombiners)
      .map(_.queue.iterator.map(_.value).toSeq)
  }

  /**
   * Return a sampled subset of this SCollection containing at most serializable `totalByteSize`.
   * Involves combine operation resulting in shuffling. All the elements of the output should fit
   * into main memory of a single worker machine.
   *
   * @return
   *   a new SCollection whose single value is an `Iterable` of the samples
   * @group transform
   */
  def sampleByteSized(totalByteSize: Long): SCollection[Iterable[T]] = {
    import self.coder
    sampleWeighted(totalByteSize, ScioUtil.elementByteSize(self.context))
  }

  /**
   * Return a sampled subset of this SCollection. Does not trigger shuffling.
   *
   * @param withReplacement
   *   if `true` the same element can be produced more than once, otherwise the same element will be
   *   sampled only once
   * @param fraction
   *   the sampling fraction
   * @param seedOpt
   *   if the same seed is set, sampling is done deterministically default to None where sampling is
   *   done non-deterministically
   * @group transform
   */
  def sample(withReplacement: Boolean, fraction: Double, seedOpt: Option[Long]): SCollection[T] = {
    import self.coder
    if (withReplacement) {
      self.parDo(new PoissonSampler[T](fraction, seedOpt))
    } else {
      self.parDo(new BernoulliSampler[T](fraction, seedOpt))
    }
  }

  /**
   * Return a sampled subset of this SCollection. Does not trigger shuffling.
   *
   * @param withReplacement
   *   if `true` the same element can be produced more than once, otherwise the same element will be
   *   sampled only once
   * @param fraction
   *   the sampling fraction
   * @group transform
   */
  def sample(withReplacement: Boolean, fraction: Double): SCollection[T] =
    sample(withReplacement, fraction, None)
}
