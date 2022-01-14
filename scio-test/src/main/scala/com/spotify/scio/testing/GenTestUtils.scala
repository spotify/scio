/*
 * Copyright 2022 Spotify AB.
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

import org.scalacheck.Gen
import org.scalacheck.rng.Seed
import org.slf4j.LoggerFactory

import scala.util.Try

/** Trait with utility methods for unit testing pipelines. */
trait GenTestUtils {
  private[this] val logger = LoggerFactory.getLogger(this.getClass)

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
