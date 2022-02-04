package com.spotify.scio.testing

import com.spotify.scio.transforms.BaseAsyncLookupDoFn
import com.spotify.scio.util.Functions
import org.apache.beam.runners.core.construction.{PTransformReplacements, ReplacementOutputs}
import org.apache.beam.sdk.runners.{
  AppliedPTransform,
  PTransformMatcher,
  PTransformOverride,
  PTransformOverrideFactory
}
import org.apache.beam.sdk.runners.PTransformOverrideFactory.{
  PTransformReplacement,
  ReplacementOutput
}
import org.apache.beam.sdk.transforms.{Create, MapElements, PTransform}
import org.apache.beam.sdk.values.{KV, PBegin, PCollection, PInput, POutput, TupleTag}

import java.util
import scala.jdk.CollectionConverters._
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

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a source with name `name` with a
   *   source producing `values`.
   */
  def ofSource[U](name: String, values: Seq[U]): PTransformOverride =
    PTransformOverride.of(
      new EqualNamePTransformMatcher(name),
      factory[PBegin, PCollection[U], PTransform[PBegin, PCollection[U]]](
        t => t.getPipeline.begin(),
        Create.of(values.asJava)
      )
    )

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform mapping elements via `fn`.
   */
  def of[T, U](name: String, fn: T => U): PTransformOverride = {
    val overrideFactory =
      factory[PCollection[T], PCollection[U], PTransform[PCollection[T], PCollection[U]]](
        t => PTransformReplacements.getSingletonMainInput(t),
        new PTransform[PCollection[T], PCollection[U]]() {
          override def expand(input: PCollection[T]): PCollection[U] =
            input.apply(MapElements.via(Functions.simpleFn(fn)))
        }
      )
    PTransformOverride.of(new EqualNamePTransformMatcher(name), overrideFactory)
  }

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform mapping keys of `mapping` to corresponding values in `mapping`.
   */
  def of[T, U](name: String, mapping: Map[T, U]): PTransformOverride =
    of[T, U](name, (t: T) => mapping(t))

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform mapping elements via `fn` and wrapping the result in a [[KV]]
   */
  def ofKV[T, U](name: String, fn: T => U): PTransformOverride =
    of[T, KV[T, U]](name, (t: T) => KV.of(t, fn(t)))

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform mapping keys of `mapping` to corresponding values in `mapping` and wrapping
   *   the result in a [[KV]]
   */
  def ofKV[T, U](name: String, mapping: Map[T, U]): PTransformOverride =
    of[T, KV[T, U]](name, mapping.map { case (k, v) => k -> KV.of(k, v) })

  /**
   * @return
   *   A [[PTransformOverride]] which when applied will override a [[PTransform]] with name `name`
   *   with a transform mapping elements via `fn` and wrapping the result in a
   *   [[BaseAsyncLookupDoFn.Try]] in a [[KV]].
   */
  def ofAsyncLookup[T, U](name: String, fn: T => U): PTransformOverride =
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
   *   with a transform mapping keys of `mapping` to corresponding values in `mapping`, and wrapping
   *   the result in a [[BaseAsyncLookupDoFn.Try]] in a [[KV]].
   */
  def ofAsyncLookup[T, U](name: String, mapping: Map[T, U]): PTransformOverride =
    ofKV[T, BaseAsyncLookupDoFn.Try[U]](
      name,
      mapping.map { case (k, v) => k -> new BaseAsyncLookupDoFn.Try(v) }
    )
}
