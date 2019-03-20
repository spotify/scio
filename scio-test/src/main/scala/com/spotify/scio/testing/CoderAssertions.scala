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

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.util.{CoderUtils, SerializableUtils}
import org.scalactic.Equality
import org.scalatest.Assertion
import org.scalatest.Matchers._

import scala.reflect.ClassTag

object CoderAssertions {
  private lazy val DefaultPipelineOptions = PipelineOptionsFactory.create()

  implicit class CoderAssertionsImplicits[T](private val value: T) extends AnyVal {
    def coderShould(coderAssertion: CoderAssertion[T])(implicit c: Coder[T],
                                                       eq: Equality[T]): Assertion =
      coderAssertion.assert(value)
  }

  trait CoderAssertion[T] {
    def assert(value: T)(implicit c: Coder[T], eq: Equality[T]): Assertion
  }

  def roundtrip[T](opts: PipelineOptions = DefaultPipelineOptions): CoderAssertion[T] =
    new CoderAssertion[T] {
      override def assert(value: T)(implicit c: Coder[T], eq: Equality[T]): Assertion = {
        val beamCoder = CoderMaterializer.beamWithDefault(c, o = opts)
        checkRoundtripWithCoder(beamCoder, value)
      }
    }

  def roundtripKryo[T: ClassTag](
    opts: PipelineOptions = DefaultPipelineOptions): CoderAssertion[T] =
    new CoderAssertion[T] {
      override def assert(value: T)(implicit c: Coder[T], eq: Equality[T]): Assertion = {
        val kryoCoder = CoderMaterializer.beamWithDefault(Coder.kryo[T], o = opts)
        checkRoundtripWithCoder(kryoCoder, value)
      }
    }

  def notFallback[T: ClassTag](opts: PipelineOptions = DefaultPipelineOptions): CoderAssertion[T] =
    new CoderAssertion[T] {
      override def assert(value: T)(implicit c: Coder[T], eq: Equality[T]): Assertion = {
        c should !==(Coder.kryo[T])
        val beamCoder = CoderMaterializer.beamWithDefault(c, o = opts)
        checkRoundtripWithCoder[T](beamCoder, value)
      }
    }

  def fallback[T: ClassTag](opts: PipelineOptions = DefaultPipelineOptions): CoderAssertion[T] =
    new CoderAssertion[T] {
      override def assert(value: T)(implicit c: Coder[T], eq: Equality[T]): Assertion = {
        c should ===(Coder.kryo[T])
        roundtripKryo(opts).assert(value)
      }
    }

  def coderIsSerializable[A](implicit c: Coder[A]): Assertion =
    coderIsSerializable(CoderMaterializer.beamWithDefault(c))

  private def coderIsSerializable[A](beamCoder: BCoder[A]): Assertion =
    noException should be thrownBy SerializableUtils.ensureSerializable(beamCoder)

  private def checkRoundtripWithCoder[T](beamCoder: BCoder[T], value: T)(
    implicit eq: Equality[T]): Assertion = {
    val bytes = CoderUtils.encodeToByteArray(beamCoder, value)
    val result = CoderUtils.decodeFromByteArray(beamCoder, bytes)

    result should ===(value)
  }
}
