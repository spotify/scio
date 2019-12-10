/*
 * Copyright 2019 Spotify AB.
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

// Example: Create, Read and Write ApproxFilters (Eg: BloomFilters)
// Usage:
//
// `sbt runMain "com.spotify.scio.examples.extra.ApproxFilterExample
// --project=[PROJECT] --runner=DirectRunner --zone=[ZONE]
// --method=[METHOD]"`
package com.spotify.scio.examples.extra

import java.io.ByteArrayOutputStream

import com.google.common.hash.{Funnel, Funnels}
import com.spotify.scio._
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.values._

// TODO add an example with case class / tuple and magnolify derivation of funnel

object ApproxFilterExamples {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val m = args("method")
    m match {
      // Example using BloomFilter as an ApproxFilter
      case "BloomFilter" => bloomFilter(sc, args)

      // Example using ScalableBloomFilter as an ApproxFilter
      case "ScalableBloomFilter" => scalableBloomFilter(sc, args)

      case _ => throw new RuntimeException(s"Invalid method $m")
    }

    sc.run()
    ()
  }

  private def bloomFilter(sc: ScioContext, args: Args): Unit = {
    // Implicit Funnel for Bloom Filters
    implicit val integerFunnel: Funnel[Int] = Funnels.integerFunnel().asInstanceOf[Funnel[Int]]

    val elements = sc.parallelize(1 to 100)

    // Single threaded build.
    val bf: SCollection[BloomFilter[Int]] = elements
      .to_(BloomFilter(fpProb = 0.1))

    // Another example
    val bf2: SCollection[BloomFilter[Int]] = elements
      .toBloomFilter(fpProb = 0.1)

    // Parallel build (useses aggregation across multiple nodes, but aggregates into one filter)
    val pbf: SCollection[BloomFilter[Int]] =
      elements.to_(BloomFilter.par(numElements = 100, fpProb = 0.1))

    // A Bloom Filter per key
    val perKeyFilter: SCollection[(Int, BloomFilter[Int])] =
      elements
        .keyBy(_ % 2) // Create a KV pair SCollection
        .groupByKey // Bring all elements per key,
        .mapValues(BloomFilter(fpProb = 0.1).build(_))

    // Another Bloom Filter per key
    val perKeyFilter2: SCollection[(Int, BloomFilter[Int])] =
      elements
        .keyBy(_ % 2) // Create a KV pair SCollection
        .toBloomFilterPerKey(fpProb = 0.1)

    // Use the exact filter as a SideInput.
    val filterSideInput: SideInput[BloomFilter[Int]] =
      bf.asSingletonSideInput(BloomFilter.empty(numElements = 100, fpProb = 0.1))

    // Serialize as bytes for persisting
    val asBytes: SCollection[Array[Byte]] = bf.map(_.toBytes)

    // Deserialize from bytes into a filter
    val deserialized: SCollection[BloomFilter[Int]] = asBytes.map(BloomFilter.fromBytes[Int](_))

    // Check for membership
    val mayBeOne: SCollection[Boolean] = deserialized.map(_.mayBeContains(1))
  }

  private def scalableBloomFilter(sc: ScioContext, args: Args): Unit = {
    // Implicit Funnel for Bloom Filters
    implicit val integerFunnel: Funnel[Int] = Funnels.integerFunnel().asInstanceOf[Funnel[Int]]

    val elements = sc.parallelize(1 to 100)

    // Single threaded build.
    val sbf: SCollection[ScalableBloomFilter[Int]] = elements
      .to_(
        ScalableBloomFilter(fpProb = 0.1,
                            initialCapacity = 100,
                            growthRate = 2,
                            tighteningRatio = 0.1)
      )

    // A ScalableBloomFilter per key
    val perKeyFilter: SCollection[(Int, ScalableBloomFilter[Int])] =
      elements
        .keyBy(_ % 2) // Create a KV pair SCollection
        .groupByKey // Bring all elements per key,
        .mapValues(
          ScalableBloomFilter(
            fpProb = 0.1,
            initialCapacity = 100,
            growthRate = 2,
            tighteningRatio = 0.1
          ).build(_)
        )

    // A ScalableBloomFilter per key (another example)
    val perKeyFilter2: SCollection[(Int, ScalableBloomFilter[Int])] =
      elements
        .keyBy(_ % 2) // Create a KV pair SCollection
        .toScalableBloomFilterPerKey(
          fpProb = 0.1,
          initialCapacity = 100,
          growthRate = 2,
          tighteningRatio = 0.1
        )

    // Add more elements to the BloomFilters.
    val moreElementsByKey: SCollection[(Int, Iterable[Int])] = sc
      .parallelize(101 to 200)
      .keyBy(_ % 2)
      .groupByKey

    val scaledUpBf: SCollection[(Int, ScalableBloomFilter[Int])] = perKeyFilter
      .join(moreElementsByKey)
      .mapValues {
        case (oldSbf: ScalableBloomFilter[Int], moreElements: Iterable[Int]) =>
          oldSbf.putAll(moreElements) // Creates a new ScalableBloomFilter
      }

//    // Serialize as bytes for persisting
//    val asBytes: SCollection[(Int, Array[Byte])] = scaledUpBf.mapValues(_.toBytes)
//
//    // Deserialize from bytes into a filter
//    val deserialized: SCollection[(Int, ScalableBloomFilter[Int])] = asBytes.mapValues {
//      serializedBytes =>
//        ScalableBloomFilter.fromBytes[Int](serializedBytes)
//    }

    // Check for membership
//    val mayBeOne: SCollection[(Int, Boolean)] = deserialized.mapValues(_.mayBeContains(1))
  }
}
// End of BloomFilter and ScalableBloomFilter Example

/**
 * Creating your own structures, and extending the [[ApproxFilter]] API
 *
 * This example uses a [[Set]] which is an approx filter which has 0 false positive and 0 false
 * negative probability.
 */
@SerialVersionUID(1L)
final case class ExactFilter[T] private (private val internal: Set[T], coder: Coder[Set[T]])
    extends ApproxFilter[T] {

  /**
   * Check if the filter may contain a given element.
   */
  override def mayBeContains(t: T): Boolean = internal.contains(t)
}

/**
 * Companion object to define implicit deserializer, and constructor that
 * returns a [[ApproxFilterBuilder]].
 */
object ExactFilter extends ApproxFilterCompanion {

//  /** Constructor that returns a Builder which can be used to create an ExactFilter. */
//  def apply[T]: ExactFilterBuilder[T] = ExactFilterBuilder()

  /** An empty filter. */
  def empty[T: Coder]: ExactFilter[T] = ExactFilter(Set.empty[T], implicitly[Coder[Set[T]]])

}

///**
// * An [[ApproxFilterBuilder]] defines how an [[ApproxFilter]] can be created.
// *
// * It can define how the filter is constructed from `Iterable` / `SCollection`.
// *
// * This allows us to separate the creation of an [[ApproxFilter]] compared to using it.
// * Hence we can have multiple optimized creation algorithms that are different from
// * the way the filter is being read.
// */
//final case class ExactFilterBuilder[T]() extends ApproxFilterBuilder[T, ExactFilter] {
//  /** Build from an Iterable */
//  override def build(it: Iterable[T]): ExactFilter[T] =
//    ExactFilter(it.toSet) // Create a Set from all elements of the iterable.
//}

///**
// * Now that we have our [[ApproxFilter]] defined, lets use it in an example pipeline.
// */
//object ExactFilterUsageExample {
//  def main(cmdlineArgs: Array[String]): Unit = {
//    val (sc, args) = ContextAndArgs(cmdlineArgs)
//
//    // Take an SCollection
//    val elements: SCollection[Int] = sc.parallelize(1 to 100)
//
//    // Create an ExactFilter from it.
//    val filter: SCollection[ExactFilter[Int]] = elements.to_(ExactFilter[Int])
//
//    // Create a per-key filter
//    val perKeyFilter: SCollection[(Int, ExactFilter[Int])] =
//      elements
//        .keyBy(_ % 2) // Create a KV pair SCollection
//        .groupByKey // Bring all elements per key,
//        .mapValues(ExactFilter[Int].build(_))
//
//    // Use the exact filter as a SideInput.
//    val filterSideInput: SideInput[ExactFilter[Int]] =
//      filter.asSingletonSideInput(ExactFilter.empty)
//
//    // Serialize as bytes for persisting
//    val asBytes: SCollection[Array[Byte]] = filter.map(_.toBytes)
//
//    // Deserialize from bytes into a filter
//    val deserialized: SCollection[ExactFilter[Int]] = asBytes.map(ExactFilter.fromBytes[Int](_))
//
//    // Check for membership
//    val mayBeOne: SCollection[Boolean] = deserialized.map(_.mayBeContains(1))
//
//    val sr: ScioResult = sc.run().waitUntilDone()
//  }
//}
// End of ExactFilter Example
