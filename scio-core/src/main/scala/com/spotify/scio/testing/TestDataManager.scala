/*
 * Copyright 2016 Spotify AB.
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

import com.spotify.scio.ScioResult
import com.spotify.scio.values.SCollection

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.{Set => MSet}

/* Inputs are Scala Iterables to be parallelized for TestPipeline */
private[scio] class TestInput(val m: Map[String, Iterable[_]]) {
  val s: MSet[String] = MSet.empty

  def apply[T](key: String): Iterable[T] = {
    require(
      m.contains(key),
      s"Missing test input: $key, available: ${m.keys.mkString("[", ", ", "]")}")
    require(!s.contains(key),
      s"There already exists test input for $key, currently " +
        s"registered inputs: ${s.mkString("[", ", ", "]")}")
    s.add(key)
    m(key).asInstanceOf[Iterable[T]]
  }

  def validate(): Unit = {
    val d = m.keySet -- s
    require(d.isEmpty, "Unmatched test input: " + d.mkString(", "))
  }
}

/* Outputs are lambdas that apply assertions on SCollections */
private[scio] class TestOutput(val m: Map[String, SCollection[_] => Unit]) {
  val s: MSet[String] = MSet.empty

  def apply[T](key: String): SCollection[T] => Unit = {
    // TODO: support Materialize outputs, maybe Materialized[T]?
    require(
      m.contains(key),
      s"Missing test output: $key, available: ${m.keys.mkString("[", ", ", "]")}")
    require(!s.contains(key),
      s"There already exists test output for $key, currently " +
        s"registered outputs: ${s.mkString("[", ", ", "]")}")
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
      s"Missing test dist cache: $key, available: ${m.keys.mkString("[", ", ", "]")}")
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

  def getInput(testId: String)
  : TestInput = getValue(testId, inputs, "reading input")

  def getOutput(testId: String)
  : TestOutput = getValue(testId, outputs, "writing output")

  def getDistCache(testId: String): TestDistCache =
    getValue(testId, distCaches, "using dist cache")

  def setup(testId: String,
            ins: Map[String, Iterable[_]],
            outs: Map[String, SCollection[_] => Unit],
            dcs: Map[DistCacheIO[_], _]): Unit = {
    inputs += (testId -> new TestInput(ins))
    outputs += (testId -> new TestOutput(outs))
    distCaches += (testId -> new TestDistCache(dcs))
  }

  def tearDown(testId: String, f: ScioResult => Unit = _ => Unit): Unit = {
    inputs.remove(testId).foreach(_.validate())
    outputs.remove(testId).foreach(_.validate())
    distCaches.remove(testId).get.validate()
    ensureClosed(testId)
    val result = results.remove(testId).get
    f(result)
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
  def apply[T](uris: Seq[String]): DistCacheIO[T] = DistCacheIO(uris.mkString("\t"))
}
