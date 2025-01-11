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

import com.spotify.scio.coders._
import org.apache.beam.sdk.coders.Coder.NonDeterministicException
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.testing.CoderProperties
import org.apache.beam.sdk.util.{CoderUtils, SerializableUtils}
import org.scalactic.Equality
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers._
import org.typelevel.scalaccompat.annotation.nowarn

import scala.reflect.ClassTag

object CoderAssertions {
  private lazy val DefaultPipelineOptions = PipelineOptionsFactory.create()

  type CoderAssertion[T] = AssertionContext[T] => Assertion
  type CoderAssertionBase = AssertionContextBase => Assertion

  case class WithOptions(opts: PipelineOptions)

  trait CustomOptionsSyntax[T] {
    def should(coderAssertion: CoderAssertion[T]): AssertionContext[T]
  }

  implicit class ValueShouldSyntax[T](value: T) {
    def coderShould(
      coderAssertion: CoderAssertion[T]
    )(implicit c: Coder[T]): AssertionContext[T] = {
      val ctx = AssertionContext(Some(value), c)
      ctx.copy(lastAssertion = Some(coderAssertion(ctx)))
    }

    def kryoCoderShould(
      coderAssertion: CoderAssertion[T]
    )(implicit ct: ClassTag[T]): AssertionContext[T] = {
      val ctx = AssertionContext(Some(value), Coder.kryo[T])
      ctx.copy(lastAssertion = Some(coderAssertion(ctx)))
    }

    def coder(
      optionsTerm: WithOptions
    )(implicit c: Coder[T]): CustomOptionsSyntax[T] = new CustomOptionsSyntax[T] {
      override def should(coderAssertion: CoderAssertion[T]): AssertionContext[T] = {
        val ctx = AssertionContext(Some(value), c, opts = optionsTerm.opts)
        ctx.copy(lastAssertion = Some(coderAssertion(ctx)))
      }
    }
  }

  implicit class CoderShouldSyntax[T](c: Coder[T]) {
    def coderShould(
      coderAssertion: CoderAssertion[T]
    ): AssertionContext[T] = {
      val ctx = AssertionContext(None, c)
      ctx.copy(lastAssertion = Some(coderAssertion(ctx)))
    }
  }

  case class AssertionContext[T](
    actualValue: Option[T],
    coder: Coder[T],
    lastAssertion: Option[Assertion] = None,
    opts: PipelineOptions = DefaultPipelineOptions
  ) extends AssertionContextBase {
    override type ValType = T

    def and(
      coderAssertion: CoderAssertion[T]
    ): AssertionContext[T] = copy(lastAssertion = Some(coderAssertion(this)))
  }

  trait AssertionContextBase {
    type ValType
    val actualValue: Option[ValType]
    val coder: Coder[ValType]
    val lastAssertion: Option[Assertion]
    val opts: PipelineOptions
    lazy val beamCoder: BCoder[ValType] = CoderMaterializer.beamWithDefault(coder, opts)
  }

  def roundtrip[T: Equality](): CoderAssertion[T] = ctx =>
    checkRoundtripWithCoder[T](ctx.beamCoder, ctx.actualValue.get)

  def roundtripToBytes[T: Equality](expectedBytes: Array[Byte]): CoderAssertion[T] = ctx =>
    checkRoundtripWithCoder[T](ctx.beamCoder, ctx.actualValue.get, expectedBytes)

  def haveCoderInstance(expectedCoder: Coder[_]): CoderAssertionBase = ctx =>
    ctx.coder should ===(expectedCoder)

  def notFallback[T: ClassTag: Equality](): CoderAssertion[T] = ctx => {
    ctx.coder should !==(Coder.kryo[T])
    checkRoundtripWithCoder(ctx.beamCoder, ctx.actualValue.get)
  }

  def fallback[T: ClassTag: Equality](): CoderAssertion[T] = ctx => {
    ctx.coder should ===(Coder.kryo[T])
    checkRoundtripWithCoder(ctx.beamCoder, ctx.actualValue.get)
  }

  def beConsistentWithEquals(): CoderAssertionBase = ctx =>
    ctx.beamCoder.consistentWithEquals() shouldBe true

  def beNotConsistentWithEquals(): CoderAssertionBase = ctx =>
    ctx.beamCoder.consistentWithEquals() shouldBe false

  def beDeterministic(): CoderAssertionBase = ctx =>
    noException should be thrownBy ctx.beamCoder.verifyDeterministic()

  def beNonDeterministic(): CoderAssertionBase = ctx =>
    a[NonDeterministicException] should be thrownBy ctx.beamCoder.verifyDeterministic()

  def beSerializable(): CoderAssertionBase = ctx =>
    noException should be thrownBy SerializableUtils.ensureSerializable(ctx.beamCoder)

  def coderIsSerializable[A](implicit c: Coder[A]): Assertion =
    c.coderShould(beSerializable()).lastAssertion.get

  def beOfType[ExpectedCoder: ClassTag]: CoderAssertionBase = ctx =>
    ctx.coder shouldBe a[ExpectedCoder]

  def materializeTo[ExpectedBeamCoder: ClassTag]: CoderAssertionBase =
    ctx => {
      ctx.beamCoder shouldBe a[MaterializedCoder[_]]
      ctx.beamCoder.asInstanceOf[MaterializedCoder[_]].bcoder shouldBe a[ExpectedBeamCoder]
    }

  def materializeToTransformOf[ExpectedBeamCoder: ClassTag]: CoderAssertionBase =
    ctx => {
      ctx.beamCoder shouldBe a[MaterializedCoder[_]]
      ctx.beamCoder.asInstanceOf[MaterializedCoder[_]].bcoder shouldBe a[TransformCoder[_, _]]
      val innerCoder =
        ctx.beamCoder.asInstanceOf[MaterializedCoder[_]].bcoder.asInstanceOf[TransformCoder[_, _]]
      innerCoder.bcoder shouldBe a[ExpectedBeamCoder]
    }

  /*
   * Checks that Beam's registerByteSizeObserver() and encode() are consistent
   * */
  def bytesCountTested[T <: Object: ClassTag](): CoderAssertion[T] =
    ctx => {
      val arr = Array(ctx.actualValue.get)
      noException should be thrownBy CoderProperties.testByteCount(
        ctx.beamCoder,
        BCoder.Context.OUTER: @nowarn("cat=deprecation"),
        arr
      )
    }

  /**
   * Verifies that for the given coder and values, the structural values are equal if and only if
   * the encoded bytes are equal. Verifies for Outer and Nested contexts
   */
  def structuralValueConsistentWithEquals(): CoderAssertionBase = ctx => {
    noException should be thrownBy CoderProperties.structuralValueConsistentWithEquals(
      ctx.beamCoder,
      ctx.actualValue.get,
      ctx.actualValue.get
    )
  }

  /** Passes all checks on Beam coder */
  def beFullyCompliant[T <: Object: ClassTag](): CoderAssertion[T] = ctx => {
    beSerializable()(ctx)
    structuralValueConsistentWithEquals()(ctx)
    beConsistentWithEquals()(ctx)
    bytesCountTested[T]().apply(ctx)
    beDeterministic()(ctx)
  }

  def beFullyCompliantNonDeterministic[T <: Object: ClassTag](): CoderAssertion[T] =
    ctx => {
      beSerializable()(ctx)
      structuralValueConsistentWithEquals()(ctx)
      beConsistentWithEquals()(ctx)
      bytesCountTested[T]().apply(ctx)
      beNonDeterministic()(ctx)
    }

  def beFullyCompliantNotConsistentWithEquals[T <: Object: ClassTag](): CoderAssertion[T] =
    ctx => {
      beSerializable()(ctx)
      structuralValueConsistentWithEquals()(ctx)
      beNotConsistentWithEquals()(ctx)
      bytesCountTested[T]().apply(ctx)
      beDeterministic()(ctx)
    }

  private def checkRoundtripWithCoder[T: Equality](
    beamCoder: BCoder[T],
    actualValue: T,
    expectedBytes: Array[Byte] = null
  ): Assertion = {
    val bytes = CoderUtils.encodeToByteArray(beamCoder, actualValue)
    if (expectedBytes != null) {
      bytes should ===(expectedBytes)
    }
    val result = CoderUtils.decodeFromByteArray(beamCoder, bytes)
    result should ===(actualValue)
  }
}
