package com.spotify.cloud.dataflow.testing

import com.google.api.services.bigquery.model.TableRow
import com.google.api.services.datastore.DatastoreV1.{Query, Entity}
import com.google.cloud.dataflow.sdk.values.PCollection
import org.apache.avro.generic.IndexedRecord

import scala.collection.mutable.{Map => MMap}

/* Inputs are Scala Iterables to be parallelized for TestPipeline */
private[dataflow] class TestInput(val m: Map[TestIO[_], Iterable[_]]) {
  def apply[T](key: TestIO[T]): Iterable[T] = m(key).asInstanceOf[Iterable[T]]
}

/* Outputs are lambdas that apply assertions on PCollections */
private[dataflow] class TestOutput(val m: Map[TestIO[_], PCollection[_] => Unit]) {
  def apply[T](key: TestIO[T]): PCollection[T] => Unit = m(key)
}

private[dataflow] class TestDistCache(val m: Map[DistCacheIO[_], _]) {
  def apply[T](key: DistCacheIO[T]): T = m(key).asInstanceOf[T]
}

private[dataflow] object TestDataManager {

  private val inputs = MMap.empty[String, TestInput]
  private val outputs = MMap.empty[String, TestOutput]
  private val distCaches = MMap.empty[String, TestDistCache]

  def getInput(testId: String) = inputs(testId)
  def getOutput(testId: String) = outputs(testId)
  def getDistCache(testId: String) = distCaches(testId)

  def setInput(testId: String, input: TestInput) = inputs += (testId -> input)
  def setOutput(testId: String, output: TestOutput) = outputs += (testId -> output)
  def setDistCache(testId: String, distCache: TestDistCache) = distCaches += (testId -> distCache)

  def unsetInput(testId: String) = inputs -= testId
  def unsetOutput(testId: String) = outputs -= testId
  def unsetDistCache(testId: String) = distCaches -= testId

}

/* For matching IO types */

class TestIO[+T] private[testing] (val key: String)

case class AvroIO[T <: IndexedRecord](path: String) extends TestIO(path)

case class BigQueryIO(tableSpecOrQuery: String) extends TestIO[TableRow](tableSpecOrQuery)

case class DatastoreIO(datasetId: String, query: Query = null) extends TestIO[Entity](s"$datasetId\t$query")

case class PubsubIO(topic: String) extends TestIO[String](topic)

case class TableRowJsonIO(path: String) extends TestIO[TableRow](path)

case class TextIO(path: String) extends TestIO[String](path)

case class DistCacheIO[T](uri: String)