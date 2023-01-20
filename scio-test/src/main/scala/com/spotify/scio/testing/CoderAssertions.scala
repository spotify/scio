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
import org.apache.beam.sdk.testing.CoderProperties
import org.apache.beam.sdk.util.{CoderUtils, SerializableUtils}
import org.scalactic.Equality
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers._

import scala.reflect.ClassTag

object CoderAssertions {
  private lazy val DefaultPipelineOptions = PipelineOptionsFactory.create()

  type CoderAssertionT[T] = AssertionContextT[T] => Assertion
  type CoderAssertion = AssertionContext => Assertion

  implicit class ValueShouldSyntax[T](value: T) {
    def coderShould(
      coderAssertion: CoderAssertionT[T]
    )(implicit c: Coder[T]): AssertionContextT[T] = {
      val ctx = AssertionContext.Concrete(Some(value), c)
      ctx.copy(lastAssertion = Some(coderAssertion(ctx)))
    }

    def kryoCoderShould(
      coderAssertion: CoderAssertionT[T]
    )(implicit eq: Equality[T], ct: ClassTag[T]): AssertionContextT[T] = {
      val ctx = AssertionContext.Concrete(Some(value), Coder.kryo[T])
      ctx.copy(lastAssertion = Some(coderAssertion(ctx)))
    }

    def coderShouldWithOpts(opts: PipelineOptions)(implicit c: Coder[T]): AssertionContextT[T] =
      AssertionContext.Concrete(Some(value), c, opts = opts)
  }

  implicit class CoderShouldSyntax[T](c: Coder[T]) {
    def coderShould(
      coderAssertion: CoderAssertionT[T]
    )(implicit c: Coder[T]): AssertionContextT[T] = {
      val ctx = AssertionContext.Concrete(None, c)
      ctx.copy(lastAssertion = Some(coderAssertion(ctx)))
    }
  }

  object AssertionContext {
    case class Concrete[T](
      actualValue: Option[T],
      coder: Coder[T],
      lastAssertion: Option[Assertion] = None,
      opts: PipelineOptions = DefaultPipelineOptions
    ) extends AssertionContextT[T]
  }

  trait AssertionContextT[T] extends AssertionContext {
    override type ValType = T
    def and(
      coderAssertion: CoderAssertionT[T]
    ): AssertionContextT[T] =
      AssertionContext.Concrete(
        actualValue,
        coder,
        Some(coderAssertion(this)),
        opts
      )
  }

  trait AssertionContext {
    type ValType
    val actualValue: Option[ValType]
    val coder: Coder[ValType]
    val lastAssertion: Option[Assertion]
    val opts: PipelineOptions
    lazy val beamCoder: BCoder[ValType] = CoderMaterializer.beamWithDefault(coder, opts)

//    def withPipelineOpts(customOpts: PipelineOptions): AssertionContext =
//      AssertionContext.Concrete(actualValue, coder, lastAssertion, customOpts)
  }

  def roundtrip[T]()(implicit eq: Equality[T]): CoderAssertionT[T] = ctx =>
    checkRoundtripWithCoder[T](ctx.beamCoder, ctx.actualValue.get)

  def roundtripToBytes[T](
    expectedBytes: Array[Byte]
  )(implicit eq: Equality[T]): CoderAssertionT[T] = ctx =>
    checkRoundtripWithCoder[T](ctx.beamCoder, ctx.actualValue.get, expectedBytes)

  def haveCoderInstance(expectedCoder: Coder[_]): CoderAssertion = ctx =>
    ctx.coder should ===(expectedCoder)

//  def roundtripKryo[T: ClassTag](): CoderAssertionT[T] = ctx => {
//    ctx.coder should ===(Coder.kryo[T])
//    checkRoundtripWithCoder(ctx.beamCoder, ctx.actualValue.get)
//  }

  def notFallback[T: ClassTag](): CoderAssertionT[T] = ctx => {
    ctx.coder should !==(Coder.kryo[T])
    checkRoundtripWithCoder(ctx.beamCoder, ctx.actualValue.get)
  }

  def fallback[T: ClassTag](): CoderAssertionT[T] = ctx => {
    ctx.coder should ===(Coder.kryo[T])
    checkRoundtripWithCoder(ctx.beamCoder, ctx.actualValue.get)
  }

  def beConsistentWithEquals(): CoderAssertion = ctx =>
    ctx.beamCoder.consistentWithEquals() shouldBe true

  def beDeterministic(): CoderAssertion = ctx =>
    noException should be thrownBy ctx.beamCoder.verifyDeterministic()

  def beSerializable(): CoderAssertion = ctx =>
    noException should be thrownBy SerializableUtils.ensureSerializable(ctx.beamCoder)

  def coderIsSerializable[A](implicit c: Coder[A]): Assertion =
    c.coderShould(beSerializable()).lastAssertion.get

  def beOfType[ExpectedCoder: ClassTag](): CoderAssertion = ctx =>
    ctx.coder shouldBe a[ExpectedCoder]

  /*
   * Checks that Beam's registerByteSizeObserver() and encode() are consistent
   * */
  def bytesCountTested[T <: Object]()(implicit ct: ClassTag[T]): CoderAssertionT[T] =
    ctx => {
      val arr = Array(ctx.actualValue.get)
      noException should be thrownBy CoderProperties.testByteCount(
        ctx.beamCoder,
        BCoder.Context.OUTER,
        arr
      )
    }

  def structuralValueConsistentWithEquals(): CoderAssertion = ctx => {
    noException should be thrownBy CoderProperties.structuralValueConsistentWithEquals(
      ctx.beamCoder,
      ctx.actualValue.get,
      ctx.actualValue.get
    )
  }

  private def checkRoundtripWithCoder[T](
    beamCoder: BCoder[T],
    actualValue: T,
    expectedBytes: Array[Byte] = null
  )(implicit
    eq: Equality[T]
  ): Assertion = {
    val bytes = CoderUtils.encodeToByteArray(beamCoder, actualValue)
    if (expectedBytes != null) {
      bytes should ===(expectedBytes)
    }
    val result = CoderUtils.decodeFromByteArray(beamCoder, bytes)
    result should ===(actualValue)
  }
}
