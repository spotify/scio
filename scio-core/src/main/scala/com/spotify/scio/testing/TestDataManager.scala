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

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ScioIO
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.{PBegin, PCollection, PInput}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.{Set => MSet}
import scala.util.{Failure, Success, Try}

/* Inputs are Scala Iterables to be parallelized for TestPipeline, or PTransforms to be applied */
private[scio] trait JobInputSource[T] {
  def toSCollection(sc: ScioContext)(implicit coder: Coder[T]): SCollection[T]
  val asIterable: Try[Iterable[T]]
}

private[scio] case class PTransformInputSource[T](
  transform: PTransform[_ >: PBegin <: PInput, PCollection[T]]
) extends JobInputSource[T] {
  override val asIterable = Failure(
    new UnsupportedOperationException(
      "PTransformInputType[T] can't be converted back to Iterable[T] as required by this TestIO"
    )
  )

  override def toSCollection(sc: ScioContext)(implicit coder: Coder[T]): SCollection[T] =
    sc.wrap(sc.applyInternal(transform))

  override def toString: String = transform.getName
}

private[scio] case class IterableInputSource[T](iterable: Iterable[T]) extends JobInputSource[T] {
  override val asIterable = Success(iterable)
  override def toSCollection(sc: ScioContext)(implicit coder: Coder[T]): SCollection[T] =
    sc.parallelize(iterable)
  override def toString: String = iterable.toString
}

private[scio] class TestInput(val m: Map[String, JobInputSource[_]]) {
  val s: MSet[String] = MSet.empty

  def apply[T](io: ScioIO[T]): JobInputSource[T] = {
    val key = io.testId
    require(
      m.contains(key),
      s"Missing test input: $key, available: ${m.keys.mkString("[", ", ", "]")}"
    )
    require(!s.contains(key), s"Test input $key has already been read from once.")
    s.add(key)
    m(key).asInstanceOf[JobInputSource[T]]
  }

  def validate(): Unit = {
    val d = m.keySet -- s
    require(d.isEmpty, "Unmatched test input: " + d.mkString(", "))
  }
}

/* Outputs are lambdas that apply assertions on SCollections */
private[scio] class TestOutput(val m: Map[String, SCollection[_] => Unit]) {
  val s: MSet[String] = MSet.empty

  def apply[T](io: ScioIO[T]): SCollection[T] => Unit = {
    // TODO: support Materialize outputs, maybe Materialized[T]?
    val key = io.testId
    require(
      m.contains(key),
      s"Missing test output: $key, available: ${m.keys.mkString("[", ", ", "]")}"
    )
    require(!s.contains(key), s"Test output $key has already been written to once.")
    s.add(key)
    m(key)
  }

  def validate(): Unit = {
    val d = m.keySet -- s
    require(d.isEmpty, "Unmatched test output: " + d.mkString(", "))
  }
}

private[scio] class TestDistCache(val m: Map[DistCacheIO[_], _]) {
  val s: MSet[DistCacheIO[_]] = MSet.empty
  def apply[T](key: DistCacheIO[T]): () => T = {
    require(
      m.contains(key),
      s"Missing test dist cache: $key, available: ${m.keys.mkString("[", ", ", "]")}"
    )
    s.add(key)
    m(key).asInstanceOf[() => T]
  }
  def validate(): Unit = {
    val d = m.keySet -- s
    require(d.isEmpty, "Unmatched test dist cache: " + d.mkString(", "))
  }
}

private[scio] object TestDataManager {

  private val inputs = TrieMap.empty[String, TestInput]
  private val outputs = TrieMap.empty[String, TestOutput]
  private val distCaches = TrieMap.empty[String, TestDistCache]
  private val closed = TrieMap.empty[String, Boolean]
  private val results = TrieMap.empty[String, ScioResult]

  private def getValue[V](key: String, m: TrieMap[String, V], ioMsg: String): V = {
    require(m.contains(key), s"Missing test data. Are you $ioMsg outside of JobTest?")
    m(key)
  }

  def getInput(testId: String): TestInput =
    getValue(testId, inputs, "reading input")

  def getOutput(testId: String): TestOutput =
    getValue(testId, outputs, "writing output")

  def getDistCache(testId: String): TestDistCache =
    getValue(testId, distCaches, "using dist cache")

  def setup(
    testId: String,
    ins: Map[String, JobInputSource[_]],
    outs: Map[String, SCollection[_] => Unit],
    dcs: Map[DistCacheIO[_], _]
  ): Unit = {
    inputs += (testId -> new TestInput(ins))
    outputs += (testId -> new TestOutput(outs))
    distCaches += (testId -> new TestDistCache(dcs))
  }

  def tearDown(testId: String, f: ScioResult => Unit = _ => ()): Unit = {
    inputs.remove(testId).foreach(_.validate())
    outputs.remove(testId).foreach(_.validate())
    distCaches.remove(testId).get.validate()
    ensureClosed(testId)
    val result = results.remove(testId).get
    f(result)
    ()
  }

  def startTest(testId: String): Unit = closed(testId) = false
  def closeTest(testId: String, result: ScioResult): Unit = {
    closed(testId) = true
    results(testId) = result
  }

  def ensureClosed(testId: String): Unit = {
    require(closed(testId), "ScioContext was not closed. Did you forget close()?")
    closed -= testId
  }

}

case class DistCacheIO[T](uri: String)

object DistCacheIO {
  def apply[T](uris: Seq[String]): DistCacheIO[T] =
    DistCacheIO(uris.mkString("\t"))
}
