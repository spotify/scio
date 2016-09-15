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

import scala.collection.mutable.{Map => MMap, Set => MSet}

/* Inputs are Scala Iterables to be parallelized for TestPipeline */
private[scio] class TestInput(val m: Map[TestIO[_], Iterable[_]]) {
  val s: MSet[TestIO[_]] = MSet.empty
  def apply[T](key: TestIO[T]): Iterable[T] = {
    s.add(key)
    m(key).asInstanceOf[Iterable[T]]
  }
  def validate(): Unit = {
    val d = m.keySet -- s
    require(d.isEmpty, "Unmatched test input: " + d.mkString(" "))
  }
}

/* Outputs are lambdas that apply assertions on PCollections */
private[scio] class TestOutput(val m: Map[TestIO[_], SCollection[_] => Unit]) {
  val s: MSet[TestIO[_]] = MSet.empty
  def apply[T](key: TestIO[T]): SCollection[T] => Unit = {
    s.add(key)
    m(key)
  }
  def validate(): Unit = {
    val d = m.keySet -- s
    require(d.isEmpty, "Unmatched test output: " + d.mkString(" "))
  }
}

private[scio] class TestDistCache(val m: Map[DistCacheIO[_], _]) {
  val s: MSet[DistCacheIO[_]] = MSet.empty
  def apply[T](key: DistCacheIO[T]): T = {
    s.add(key)
    m(key).asInstanceOf[T]
  }
  def validate(): Unit = {
    val d = m.keySet -- s
    require(d.isEmpty, "Unmatched test dist cache: " + d.mkString(" "))
  }
}

private[scio] object TestDataManager {

  private val inputs = MMap.empty[String, TestInput]
  private val outputs = MMap.empty[String, TestOutput]
  private val distCaches = MMap.empty[String, TestDistCache]

  def getInput(testId: String): TestInput = inputs(testId)
  def getOutput(testId: String): TestOutput = outputs(testId)
  def getDistCache(testId: String): TestDistCache = distCaches(testId)

  def setInput(testId: String, input: TestInput): Unit = inputs += (testId -> input)
  def setOutput(testId: String, output: TestOutput): Unit = outputs += (testId -> output)
  def setDistCache(testId: String, distCache: TestDistCache): Unit =
    distCaches += (testId -> distCache)

  def unsetInput(testId: String): Unit = {
    inputs(testId).validate()
    inputs -= testId
  }
  def unsetOutput(testId: String): Unit = {
    outputs(testId).validate()
    outputs -= testId
  }
  def unsetDistCache(testId: String): Unit = {
    distCaches(testId).validate()
    distCaches -= testId
  }

}

/* For matching IO types */

class TestIO[+T] private[scio] (val key: String)

case class ObjectFileIO[T](path: String) extends TestIO(path)

case class AvroIO[T](path: String) extends TestIO(path)

case class BigQueryIO(tableSpecOrQuery: String) extends TestIO[TableRow](tableSpecOrQuery)

case class DatastoreIO(projectId: String, query: Query = null, namespace: String = null)
  extends TestIO[Entity](s"$projectId\t$query\t$namespace")

case class PubsubIO(topic: String) extends TestIO[String](topic)

case class TableRowJsonIO(path: String) extends TestIO[TableRow](path)

case class TextIO(path: String) extends TestIO[String](path)

case class DistCacheIO[T](uri: String)
