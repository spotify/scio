package com.spotify.scio

import org.apache.beam.sdk.PipelineResult
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

trait LineageProducer {
  def addSource(schema: String, id: String, extra: String = null): Unit
  def addSink(schema: String, id: String, extra: String = null): Unit
  def commit(result: PipelineResult): Unit
}

object LineageProducer extends LineageProducer {

  private val producers: ArrayBuffer[LineageProducer] = new ArrayBuffer

  def addProducer(producer: LineageProducer): Unit =
    producers.addOne(producer)

  def addSource(schema: String, id: String, extra: String = null): Unit =
    producers.foreach(_.addSource(schema, id, extra))

  def addSink(schema: String, id: String, extra: String = null): Unit =
    producers.foreach(_.addSink(schema, id, extra))

  def commit(result: PipelineResult): Unit =
    producers.foreach(_.commit(result))

}

object MetricLineageProducer

class MetricLineageProducer() extends LineageProducer {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // Thread-safe sets to collect unique sources and sinks  
  private val sources: ConcurrentHashMap[String, java.lang.Boolean] = new ConcurrentHashMap()
  private val sinks: ConcurrentHashMap[String, java.lang.Boolean] = new ConcurrentHashMap()

  override def addSource(schema: String, id: String, extra: String = null): Unit = {
    val key = s"$schema:$id"
    val wasAbsent = sources.putIfAbsent(key, java.lang.Boolean.TRUE) == null
    if (wasAbsent) {
      logger.info(s"Tracked source: $key")
    }
  }

  override def addSink(schema: String, id: String, extra: String = null): Unit = {
    val key = s"$schema:$id"
    val wasAbsent = sinks.putIfAbsent(key, java.lang.Boolean.TRUE) == null
    if (wasAbsent) {
      logger.info(s"Tracked sink: $key")
    }
  }

  override def commit(result: PipelineResult): Unit = {
    logger.info(s"Pipeline lineage summary:")
    logger.info(s"Sources: ${sources.keySet().asScala.toSet}")
    logger.info(s"Sinks: ${sinks.keySet().asScala.toSet}")
    
    // TODO: Save lineage data to file or external system
    // e.g., write to JSON file, send to monitoring system, etc.
  }
}
