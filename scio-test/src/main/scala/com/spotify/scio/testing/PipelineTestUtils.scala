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

package com.spotify.scio.testing

import org.apache.beam.sdk.options._
import com.spotify.scio._
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import org.slf4j.LoggerFactory

import scala.util.Try

/** Trait with utility methods for unit testing pipelines. */
trait PipelineTestUtils {
  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Test pipeline components with a [[ScioContext]].
   * @param fn
   *   code that tests the components and verifies the result
   *
   * {{{
   * runWithContext { sc =>
   *   sc.parallelize(Seq(1, 2, 3)).sum should containSingleValue (6)
   * }
   * }}}
   */
  def runWithContext[T](fn: ScioContext => T): ScioExecutionContext = {
    val sc = ScioContext.forTest()
    fn(sc)
    sc.run()
  }

  def runWithRealContext[T](
    options: PipelineOptions
  )(fn: ScioContext => T): ScioExecutionContext = {
    val sc = ScioContext(options)
    fn(sc)
    sc.run()
  }

  /**
   * Test pipeline components with in-memory data.
   *
   * Input data is passed to `fn` as an [[com.spotify.scio.values.SCollection SCollection]] and the
   * result [[com.spotify.scio.values.SCollection SCollection]] from `fn` is extracted and to be
   * verified.
   *
   * @param data
   *   input data
   * @param fn
   *   transform to be tested
   * @return
   *   output data
   *
   * {{{
   * runWithData(Seq(1, 2, 3)) { p =>
   *   p.sum
   * } shouldBe Seq(6)
   * }}}
   */
  def runWithData[T: Coder, U: Coder](
    data: Iterable[T]
  )(fn: SCollection[T] => SCollection[U]): Seq[U] =
    runWithLocalOutput(sc => fn(sc.parallelize(data)))._2

  /**
   * Test pipeline components with in-memory data.
   *
   * Input data is passed to `fn` as [[com.spotify.scio.values.SCollection SCollection]] s and the
   * result [[com.spotify.scio.values.SCollection SCollection]] from `fn` is extracted and to be
   * verified.
   *
   * @param data1
   *   input data
   * @param data2
   *   input data
   * @param fn
   *   transform to be tested
   * @return
   *   output data
   */
  def runWithData[T1: Coder, T2: Coder, U: Coder](data1: Iterable[T1], data2: Iterable[T2])(
    fn: (SCollection[T1], SCollection[T2]) => SCollection[U]
  ): Seq[U] =
    runWithLocalOutput(sc => fn(sc.parallelize(data1), sc.parallelize(data2)))._2

  /**
   * Test pipeline components with in-memory data.
   *
   * Input data is passed to `fn` as [[com.spotify.scio.values.SCollection SCollection]] s and the
   * result [[com.spotify.scio.values.SCollection SCollection]] from `fn` is extracted and to be
   * verified.
   *
   * @param data1
   *   input data
   * @param data2
   *   input data
   * @param data3
   *   input data
   * @param fn
   *   transform to be tested
   * @return
   *   output data
   */
  def runWithData[T1: Coder, T2: Coder, T3: Coder, U: Coder](
    data1: Iterable[T1],
    data2: Iterable[T2],
    data3: Iterable[T3]
  )(fn: (SCollection[T1], SCollection[T2], SCollection[T3]) => SCollection[U]): Seq[U] =
    runWithLocalOutput { sc =>
      fn(sc.parallelize(data1), sc.parallelize(data2), sc.parallelize(data3))
    }._2

  /**
   * Test pipeline components with in-memory data.
   *
   * Input data is passed to `fn` as [[com.spotify.scio.values.SCollection SCollection]] s and the
   * result [[com.spotify.scio.values.SCollection SCollection]] from `fn` is extracted and to be
   * verified.
   *
   * @param data1
   *   input data
   * @param data2
   *   input data
   * @param data3
   *   input data
   * @param data4
   *   input data
   * @param fn
   *   transform to be tested
   * @return
   *   output data
   */
  def runWithData[T1: Coder, T2: Coder, T3: Coder, T4: Coder, U: Coder](
    data1: Iterable[T1],
    data2: Iterable[T2],
    data3: Iterable[T3],
    data4: Iterable[T4]
  )(
    fn: (SCollection[T1], SCollection[T2], SCollection[T3], SCollection[T4]) => SCollection[U]
  ): Seq[U] =
    runWithLocalOutput { sc =>
      fn(sc.parallelize(data1), sc.parallelize(data2), sc.parallelize(data3), sc.parallelize(data4))
    }._2

  /**
   * Test pipeline components with a [[ScioContext]] and materialized resulting collection.
   *
   * The result [[com.spotify.scio.values.SCollection SCollection]] from `fn` is extracted and to be
   * verified.
   *
   * @param fn
   *   transform to be tested
   * @return
   *   a tuple containing the [[ScioResult]] and the materialized result of fn as a
   *   [[scala.collection.Seq Seq]]
   */
  def runWithLocalOutput[U](fn: ScioContext => SCollection[U]): (ScioResult, Seq[U]) = {
    val sc = ScioContext()
    val f = fn(sc).materialize
    val result: ScioResult = sc.run().waitUntilFinish() // block non-test runner
    (result, result.tap(f).value.toSeq)
  }

  /**
   * Generate an instance of `A` from `genA` using a random seed.
   *
   * @return
   *   The result of `fn` applied to the generated `A`, or `None` on generation failure.
   */
  def withGen[A, T](genA: Gen[A])(
    fn: A => T
  )(implicit line: sourcecode.Line, name: sourcecode.FileName): Option[T] =
    withGen(genA, Seed.random())(fn)(line, name)

  /**
   * Generate an instance of `A` from `genA` using a the seed specified by `base64Seed`.
   *
   * @return
   *   The result of `fn` applied to the generated `A`, or `None` on generation failure.
   */
  def withGen[A, T](genA: Gen[A], base64Seed: String)(
    fn: A => T
  )(implicit line: sourcecode.Line, name: sourcecode.FileName): Option[T] =
    withGen(genA, Seed.fromBase64(base64Seed).get)(fn)(line, name)

  /**
   * Generate an instance of `A` from `genA` using a the seed specified by `seed`.
   *
   * @return
   *   The result of `fn` applied to the generated `A`, or `None` on generation failure.
   */
  def withGen[A, T](genA: Gen[A], seed: Seed)(
    fn: A => T
  )(implicit line: sourcecode.Line, name: sourcecode.FileName): Option[T] = {
    genA.apply(Gen.Parameters.default, seed) match {
      case None =>
        logger.error(
          s"Failed to generate a valid value at ${name.value}:${line.value}. " +
            s"Consider rewriting Gen instances to be less failure-prone. " +
            s"Seed: ${seed.toBase64}"
        )
        None
      case Some(a) =>
        val r = Try(fn(a)).map(Some(_))
        if (r.isFailure)
          logger.error(s"Failure at ${name.value}:${line.value}. Seed: ${seed.toBase64}")
        r.get
    }
  }
}
