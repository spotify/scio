/*
 * Copyright 2022 Spotify AB
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

package com.spotify.scio.testing

import java.lang.{Iterable => JIterable}
import java.util

import com.spotify.scio.transforms.BaseAsyncLookupDoFn
import com.spotify.scio.util.Functions
import org.apache.beam.runners.core.construction.{PTransformReplacements, ReplacementOutputs}
import org.apache.beam.sdk.runners.PTransformOverrideFactory.{
  PTransformReplacement,
  ReplacementOutput
}
import org.apache.beam.sdk.runners.{
  AppliedPTransform,
  PTransformMatcher,
  PTransformOverride,
  PTransformOverrideFactory
}
import org.apache.beam.sdk.transforms._
import org.apache.beam.sdk.values._

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/** Matches a [[PTransform]] with exactly name `name`. */
class EqualNamePTransformMatcher(val name: String) extends PTransformMatcher {
  override def matches(application: AppliedPTransform[_, _, _]): Boolean =
    name.equals(application.getFullName)
  // beams retains the original Node name, so by default this matcher will match the replaced transform during validation. override this behavior.
  // see https://github.com/apache/beam/blob/master/sdks/java/core/src/main/java/org/apache/beam/sdk/runners/TransformHierarchy.java#L115
  override def matchesDuringValidation(application: AppliedPTransform[_, _, _]): Boolean = false
}

object TransformOverride {
  private def factory[In <: PInput, Out <: POutput, XForm <: PTransform[In, Out]](
    inFn: AppliedPTransform[In, Out, XForm] => In,
    replacement: XForm
  ): PTransformOverrideFactory[In, Out, XForm] =
    new PTransformOverrideFactory[In, Out, XForm]() {
      override def mapOutputs(
        outputs: util.Map[TupleTag[_], PCollection[_]],
        newOutput: Out
      ): util.Map[PCollection[_], ReplacementOutput] =
        ReplacementOutputs.singleton(outputs, newOutput)
      override def getReplacementTransform(
        transform: AppliedPTransform[In, Out, XForm]
      ): PTransformReplacement[In, Out] =
        PTransformReplacement.of(inFn(transform), replacement)
    }

  private val primitiveMapping = Map[Class[_], Class[_]](
    java.lang.Boolean.TYPE -> classOf[java.lang.Boolean],
    java.lang.Character.TYPE -> classOf[java.lang.Character],
    java.lang.Byte.TYPE -> classOf[java.lang.Byte],
    java.lang.Short.TYPE -> classOf[java.lang.Short],
    java.lang.Integer.TYPE -> classOf[java.lang.Integer],
    java.lang.Long.TYPE -> classOf[java.lang.Long],
    java.lang.Float.TYPE -> classOf[java.lang.Float],
    java.lang.Double.TYPE -> classOf[java.lang.Double]
  )

  private def typeValidation[A, B](
    expectedIn: Class[B],
    actualIn: Class[A],
    failMsg: String
  ): Unit = {
    // get normal java types instead of primitives
    val expected = primitiveMapping.getOrElse(expectedIn, expectedIn)
    val actual = primitiveMapping.getOrElse(actualIn, actualIn)
    if (!expected.isAssignableFrom(actual))
      throw new IllegalArgumentException(s"$failMsg Expected: $expected Found: $actual")
  }

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a source with name `name` with a
   *   source producing `values`.
   */
  def ofSource[U](name: String, values: Seq[U]): PTransformOverride =
    PTransformOverride.of(
      new EqualNamePTransformMatcher(name),
      factory[PBegin, PCollection[U], PTransform[PBegin, PCollection[U]]](
        inFn = t => t.getPipeline.begin(),
        replacement = Create.of(values.asJava)
      )
    )

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform mapping elements via `fn`.
   */
  def of[T: ClassTag, U](name: String, fn: T => U): PTransformOverride = {
    val wrappedFn: T => U = fn.compose { t: T =>
      typeValidation(
        implicitly[ClassTag[T]].runtimeClass,
        t.getClass,
        s"Input for override transform $name does not match pipeline transform."
      )
      t
    }

    val overrideFactory =
      factory[PCollection[T], PCollection[U], PTransform[PCollection[T], PCollection[U]]](
        PTransformReplacements.getSingletonMainInput,
        new PTransform[PCollection[T], PCollection[U]]() {
          override def expand(input: PCollection[T]): PCollection[U] =
            input.apply(MapElements.via(Functions.simpleFn(wrappedFn)))
        }
      )
    PTransformOverride.of(new EqualNamePTransformMatcher(name), overrideFactory)
  }

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform flat-mapping elements via `fn`.
   */
  def ofIter[T: ClassTag, U](name: String, fn: T => Iterable[U]): PTransformOverride = {
    val wrappedFn: T => JIterable[U] = fn
      .compose { t: T =>
        typeValidation(
          implicitly[ClassTag[T]].runtimeClass,
          t.getClass,
          s"Input for override transform $name does not match pipeline transform."
        )
        t
      }
      .andThen(_.asJava)

    val overrideFactory =
      factory[PCollection[T], PCollection[U], PTransform[PCollection[T], PCollection[U]]](
        PTransformReplacements.getSingletonMainInput,
        new PTransform[PCollection[T], PCollection[U]]() {
          override def expand(input: PCollection[T]): PCollection[U] = {
            val inferableFn = new InferableFunction[T, JIterable[U]] {
              override def apply(input: T): JIterable[U] = wrappedFn.apply(input)
            }
            input.apply(FlatMapElements.via(inferableFn))
          }
        }
      )
    PTransformOverride.of(new EqualNamePTransformMatcher(name), overrideFactory)
  }

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform mapping keys of `mapping` to corresponding values in `mapping`.
   */
  def of[T: ClassTag, U](name: String, mapping: Map[T, U]): PTransformOverride =
    of[T, U](name, (t: T) => mapping(t))

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform flat-mapping keys of `mapping` to corresponding repeated values in
   *   `mapping`.
   */
  def ofIter[T: ClassTag, U](name: String, mapping: Map[T, Iterable[U]]): PTransformOverride =
    ofIter[T, U](name, (t: T) => mapping(t))

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform mapping elements via `fn` and wrapping the result in a [[KV]]
   */
  def ofKV[T: ClassTag, U](name: String, fn: T => U): PTransformOverride =
    of[T, KV[T, U]](name, (t: T) => KV.of(t, fn(t)))

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform flat-mapping elements via `fn` and wrapping the result in a [[KV]]
   */
  def ofIterKV[T: ClassTag, U](name: String, fn: T => Iterable[U]): PTransformOverride =
    ofIter[T, KV[T, U]](name, (t: T) => fn(t).map(KV.of(t, _)))

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform mapping keys of `mapping` to corresponding values in `mapping` and wrapping
   *   the result in a [[KV]]
   */
  def ofKV[T: ClassTag, U](name: String, mapping: Map[T, U]): PTransformOverride =
    of[T, KV[T, U]](name, mapping.map { case (k, v) => k -> KV.of(k, v) })

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform flat-mapping keys of `mapping` to corresponding values in `mapping` and
   *   wrapping the result in a [[KV]]
   */
  def ofIterKV[T: ClassTag, U](name: String, mapping: Map[T, Iterable[U]]): PTransformOverride =
    ofIter[T, KV[T, U]](name, mapping.map { case (k, iterV) => k -> iterV.map(KV.of(k, _)) })

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform mapping elements via `fn` and wrapping the result in a
   *   [[BaseAsyncLookupDoFn.Try]] in a [[KV]].
   */
  def ofAsyncLookup[T: ClassTag, U](name: String, fn: T => U): PTransformOverride =
    ofKV[T, BaseAsyncLookupDoFn.Try[U]](
      name,
      (t: T) =>
        Try(fn(t)) match {
          case Success(value) => new BaseAsyncLookupDoFn.Try[U](value)
          case Failure(ex)    => new BaseAsyncLookupDoFn.Try[U](ex)
        }
    )

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform flat-mapping elements via `fn` and wrapping the result in a
   *   [[BaseAsyncLookupDoFn.Try]] in a [[KV]].
   */
  def ofIterAsyncLookup[T: ClassTag, U](name: String, fn: T => Iterable[U]): PTransformOverride =
    ofIterKV[T, BaseAsyncLookupDoFn.Try[U]](
      name,
      (t: T) =>
        fn(t).map(i =>
          Try(i) match {
            case Success(value) => new BaseAsyncLookupDoFn.Try[U](value)
            case Failure(ex)    => new BaseAsyncLookupDoFn.Try[U](ex)
          }
        )
    )

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform mapping keys of `mapping` to corresponding values in `mapping`, and wrapping
   *   the result in a [[BaseAsyncLookupDoFn.Try]] in a [[KV]].
   */
  def ofAsyncLookup[T: ClassTag, U](name: String, mapping: Map[T, U]): PTransformOverride =
    ofKV[T, BaseAsyncLookupDoFn.Try[U]](
      name,
      mapping.map { case (k, v) => k -> new BaseAsyncLookupDoFn.Try(v) }
    )

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform flat-mapping keys of `mapping` to corresponding values in `mapping`, and
   *   wrapping the result in a [[BaseAsyncLookupDoFn.Try]] in a [[KV]].
   */
  def ofIterAsyncLookup[T: ClassTag, U](
    name: String,
    mapping: Map[T, Iterable[U]]
  ): PTransformOverride =
    ofIterKV[T, BaseAsyncLookupDoFn.Try[U]](
      name,
      mapping.map { case (k, v) => k -> v.map(i => new BaseAsyncLookupDoFn.Try(i)) }
    )
}
