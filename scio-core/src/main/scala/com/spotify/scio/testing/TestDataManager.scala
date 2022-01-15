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

import com.google.common.util.concurrent.{Futures, ListenableFuture}
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ScioIO
import com.spotify.scio.transforms.DoFnWithResource.ResourceType
import com.spotify.scio.transforms.GuavaAsyncDoFn
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.beam.sdk.coders
import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.transforms.{PTransform, ParDo}
import org.apache.beam.sdk.values.PCollection
import org.apache.commons.io.output.NullOutputStream

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.{Set => MSet}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/* Inputs are Scala Iterables to be parallelized for TestPipeline, or PTransforms to be applied */
sealed private[scio] trait JobInputSource[T] {
  def toSCollection(sc: ScioContext): SCollection[T]
  val asIterable: Try[Iterable[T]]
}

final private[scio] case class TestStreamInputSource[T: Coder](
  stream: TestStream[T]
) extends JobInputSource[T] {
  override val asIterable: Try[Iterable[T]] = Failure(
    new UnsupportedOperationException(
      s"Test input $this can't be converted to Iterable[T] to test this ScioIO type"
    )
  )

  override def toSCollection(sc: ScioContext): SCollection[T] =
    sc.applyTransform(stream)

  override def toString: String = s"TestStream(${stream.getEvents})"
}

final private[scio] case class IterableInputSource[T: Coder](
  iterable: Iterable[T]
) extends JobInputSource[T] {
  override val asIterable: Success[Iterable[T]] = Success(iterable)
  override def toSCollection(sc: ScioContext): SCollection[T] =
    sc.parallelize(iterable)
  override def toString: String = iterable.toString
}

private[scio] class TestInput(val m: Map[String, JobInputSource[_]]) {
  val s: MSet[String] =
    java.util.concurrent.ConcurrentHashMap.newKeySet[String]().asScala

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
private[scio] class TestOutput(val m: Map[String, SCollection[_] => Any]) {
  val s: MSet[String] =
    java.util.concurrent.ConcurrentHashMap.newKeySet[String]().asScala

  def apply[T](io: ScioIO[T]): SCollection[T] => Any = {
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
  val s: MSet[DistCacheIO[_]] =
    java.util.concurrent.ConcurrentHashMap.newKeySet[DistCacheIO[_]]().asScala

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

private[scio] class TestTransform(val m: Map[String, Map[_, _]]) {
  val s: MSet[String] =
    java.util.concurrent.ConcurrentHashMap.newKeySet[String]().asScala

  def apply[T, U](
    t: MockTransform[T, U],
    tCoder: coders.Coder[T],
    uCoder: coders.Coder[U]
  ): PTransform[_ >: PCollection[T], PCollection[U]] = {
    val key = t.name
    require(
      m.contains(key),
      s"Missing mock transform: $key, available: ${m.keys.mkString("[", ", ", "]")}"
    )
    require(!s.contains(key), s"Mock transform $key has already been used once.")
    s.add(key)
    val xxx = m(key)
    if (xxx.nonEmpty) {
      try {
        tCoder.encode(xxx.keys.head.asInstanceOf[T], NullOutputStream.NULL_OUTPUT_STREAM)
      } catch {
        case e: ClassCastException =>
          throw new IllegalArgumentException(
            s"Input for mock transform $key does not match pipeline transform. Found: ${xxx.keys.head.getClass}",
            e
          )
      }
      try {
        uCoder.encode(xxx.values.head.asInstanceOf[U], NullOutputStream.NULL_OUTPUT_STREAM)
      } catch {
        case e: ClassCastException =>
          throw new IllegalArgumentException(
            s"Output for mock transform $key does not match pipeline transform. Found: ${xxx.values.head.getClass}",
            e
          )
      }
    }
    val values = xxx.asInstanceOf[Map[T, U]]
    val xform: PTransform[_ >: PCollection[T], PCollection[U]] = ParDo.of(
      new GuavaAsyncDoFn[T, U, Unit]() {
        override def processElement(input: T): ListenableFuture[U] =
          Futures.immediateFuture(values(input))
        override def getResourceType: ResourceType = ResourceType.PER_CLASS
        override def createResource(): Unit = ()
      }
    )
    xform
  }

  def validate(): Unit = {
    val d = m.keySet -- s
    require(d.isEmpty, "Unmatched mock transform: " + d.mkString(", "))
  }
}

private[scio] object TestDataManager {
  private val inputs = TrieMap.empty[String, TestInput]
  private val outputs = TrieMap.empty[String, TestOutput]
  private val distCaches = TrieMap.empty[String, TestDistCache]
  private val closed = TrieMap.empty[String, Boolean]
  private val results = TrieMap.empty[String, ScioResult]
  private val mockedTransforms = TrieMap.empty[String, TestTransform]

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

  def getTransform(testId: String): TestTransform =
    getValue(testId, mockedTransforms, "using transform")

  def setup(
    testId: String,
    ins: Map[String, JobInputSource[_]],
    outs: Map[String, SCollection[_] => Any],
    dcs: Map[DistCacheIO[_], _],
    xforms: Map[String, Map[_, _]]
  ): Unit = {
    inputs += (testId -> new TestInput(ins))
    outputs += (testId -> new TestOutput(outs))
    distCaches += (testId -> new TestDistCache(dcs))
    mockedTransforms += (testId -> new TestTransform(xforms))
  }

  def tearDown(testId: String, f: ScioResult => Unit = _ => ()): Unit = {
    inputs.remove(testId).foreach(_.validate())
    outputs.remove(testId).foreach(_.validate())
    distCaches.remove(testId).get.validate()
    mockedTransforms.remove(testId).foreach(_.validate())
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
    require(closed(testId), "ScioContext was not executed. Did you forget .run()?")
    closed -= testId
  }
}

case class DistCacheIO[T](uri: String)

object DistCacheIO {
  def apply[T](uris: Seq[String]): DistCacheIO[T] =
    DistCacheIO(uris.mkString("\t"))
}

case class MockTransform[T, U](name: String)
