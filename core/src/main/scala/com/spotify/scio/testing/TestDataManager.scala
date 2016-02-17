package com.spotify.scio.testing

import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.datastore.DatastoreV1.{Entity, Query}
import com.spotify.scio.values.SCollection

import scala.collection.mutable.{Map => MMap}

/* Inputs are Scala Iterables to be parallelized for TestPipeline */
private[scio] class TestInput(val m: Map[TestIO[_], Iterable[_]]) {
  def apply[T](key: TestIO[T]): Iterable[T] = m(key).asInstanceOf[Iterable[T]]
}

/* Outputs are lambdas that apply assertions on PCollections */
private[scio] class TestOutput(val m: Map[TestIO[_], SCollection[_] => Unit]) {
  def apply[T](key: TestIO[T]): SCollection[T] => Unit = m(key)
}

private[scio] class TestDistCache(val m: Map[DistCacheIO[_], _]) {
  def apply[T](key: DistCacheIO[T]): T = m(key).asInstanceOf[T]
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
  def setDistCache(testId: String, distCache: TestDistCache): Unit = distCaches += (testId -> distCache)

  def unsetInput(testId: String): Unit = inputs -= testId
  def unsetOutput(testId: String): Unit = outputs -= testId
  def unsetDistCache(testId: String): Unit = distCaches -= testId

}

/* For matching IO types */

class TestIO[+T] private[scio] (val key: String)

case class ObjectFileIO[T](path: String) extends TestIO(path)

case class AvroIO[T](path: String) extends TestIO(path)

case class BigQueryIO(tableSpecOrQuery: String) extends TestIO[TableRow](tableSpecOrQuery)

case class DatastoreIO(datasetId: String, query: Query = null) extends TestIO[Entity](s"$datasetId\t$query")

case class PubsubIO(topic: String) extends TestIO[String](topic)

case class TableRowJsonIO(path: String) extends TestIO[TableRow](path)

case class TextIO(path: String) extends TestIO[String](path)

case class DistCacheIO[T](uri: String)
