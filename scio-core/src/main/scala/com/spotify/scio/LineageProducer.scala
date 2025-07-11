package com.spotify.scio

import org.apache.beam.sdk.PipelineResult
import org.apache.beam.sdk.metrics.{Counter, Metrics}

import scala.collection.mutable.ArrayBuffer

trait LineageProducer {
  def addSource(id: String, extra: String = null): Unit
  def addSink(id: String, extra: String = null): Unit
  def commit(result: PipelineResult): Unit
}

object LineageProducer extends LineageProducer {

  private val producers: ArrayBuffer[LineageProducer] = new ArrayBuffer

  def addProducer(producer: LineageProducer): Unit =
    producers.addOne(producer)

  def addSource(id: String, extra: String = null): Unit =
    producers.foreach(_.addSource(id, extra))

  def addSink(id: String, extra: String = null): Unit =
    producers.foreach(_.addSink(id, extra))

  def commit(result: PipelineResult): Unit =
    producers.foreach(_.commit(result))

}

class MetricLineageProducer extends LineageProducer {

  private val counters: scala.collection.mutable.Map[String, Counter] =
    scala.collection.mutable.Map.empty

  override def addSource(id: String, extra: String = null): Unit = {
    val key = s"source.$id"
    val counter = counters.getOrElseUpdate(key, Metrics.counter("scio_lineage", key))
    counter.inc(1)
  }

  override def addSink(id: String, extra: String = null): Unit = {
    val key = s"sink.$id"
    val counter = counters.getOrElseUpdate(key, Metrics.counter("scio_lineage", key))
    counter.inc(1)
  }

  override def commit(result: PipelineResult): Unit = {
    /// save counters to file....
  }
}
