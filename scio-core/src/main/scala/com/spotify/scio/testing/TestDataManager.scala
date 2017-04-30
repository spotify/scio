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

import com.google.api.services.bigquery.model.TableRow
import com.google.datastore.v1.{Entity, Query}
import com.spotify.scio.values.SCollection

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.{Set => MSet}

/* Inputs are Scala Iterables to be parallelized for TestPipeline */
private[scio] class TestInput(val m: Map[TestIO[_], Iterable[_]]) {
  val s: MSet[TestIO[_]] = MSet.empty
  def apply[T](key: TestIO[T]): Iterable[T] = {
    require(
      m.contains(key),
      s"Missing test input: $key, available: ${m.keys.mkString("[", ", ", "]")}")
    s.add(key)
    m(key).asInstanceOf[Iterable[T]]
  }
  def validate(): Unit = {
    val d = m.keySet -- s
    require(d.isEmpty, "Unmatched test input: " + d.mkString(", "))
  }
}

/* Outputs are lambdas that apply assertions on SCollections */
private[scio] class TestOutput(val m: Map[TestIO[_], SCollection[_] => Unit]) {
  val s: MSet[TestIO[_]] = MSet.empty
  def apply[T](key: TestIO[T]): SCollection[T] => Unit = {
    if (key.key.contains("scio-materialize-")) {
      // dummy matcher for materialize output
      _ => Unit
    } else {
      require(
        m.contains(key),
        s"Missing test output: $key, available: ${m.keys.mkString("[", ", ", "]")}")
      s.add(key)
      m(key)
    }
  }
  def validate(): Unit = {
    val d = m.keySet -- s
    require(d.isEmpty, "Unmatched test output: " + d.mkString(", "))
  }
}

private[scio] class TestDistCache(val m: Map[DistCacheIO[_], _]) {
  val s: MSet[DistCacheIO[_]] = MSet.empty
  def apply[T](key: DistCacheIO[T]): T = {
    require(
      m.contains(key),
      s"Missing test dist cache: $key, available: ${m.keys.mkString("[", ", ", "]")}")
    s.add(key)
    m(key).asInstanceOf[T]
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

  def getInput(testId: String): TestInput = inputs(testId)
  def getOutput(testId: String): TestOutput = outputs(testId)
  def getDistCache(testId: String): TestDistCache = distCaches(testId)

  def setup(testId: String,
            ins: Map[TestIO[_], Iterable[_]],
            outs: Map[TestIO[_], SCollection[_] => Unit],
            dcs: Map[DistCacheIO[_], _]): Unit = {
    inputs += (testId -> new TestInput(ins))
    outputs += (testId -> new TestOutput(outs))
    distCaches += (testId -> new TestDistCache(dcs))
  }

  def tearDown(testId: String): Unit = {
    inputs.remove(testId).get.validate()
    outputs.remove(testId).get.validate()
    distCaches.remove(testId).get.validate()
    ensureClosed(testId)
  }

  def startTest(testId: String): Unit = closed(testId) = false
  def closeTest(testId: String): Unit = closed(testId) = true
  def ensureClosed(testId: String): Unit = {
    require(closed(testId), "ScioContext was not closed. Did you forget close()?")
    closed -= testId
  }

}

/* For matching IO types */

class TestIO[+T] private[scio] (val key: String) {
  require(key != null, s"$this has null key")
  require(!key.isEmpty, s"$this has empty string key")
}

case class ObjectFileIO[T](path: String) extends TestIO[T](path)

case class AvroIO[T](path: String) extends TestIO[T](path)

case class BigQueryIO(tableSpecOrQuery: String) extends TestIO[TableRow](tableSpecOrQuery)

case class DatastoreIO(projectId: String, query: Query = null, namespace: String = null)
  extends TestIO[Entity](projectId)

case class ProtobufIO[T](path: String) extends TestIO[T](path)

case class PubsubIO[T](topic: String) extends TestIO[T](topic)

case class TableRowJsonIO(path: String) extends TestIO[TableRow](path)

case class TextIO(path: String) extends TestIO[String](path)

case class TFRecordIO(path: String) extends TestIO[Array[Byte]](path)

case class DistCacheIO[T](uri: String)

object DistCacheIO {
  def apply[T](uris: Seq[String]): DistCacheIO[T] = DistCacheIO(uris.mkString("\t"))
}

case class CustomIO[T](name: String) extends TestIO[T](name)
