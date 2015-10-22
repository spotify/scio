package com.spotify.scio.sinks

import java.lang.Iterable
import java.util.UUID

import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.io.Sink.{WriteOperation, Writer}
import com.google.cloud.dataflow.sdk.io.{Sink => GSink}
import com.google.cloud.dataflow.sdk.options.PipelineOptions
import com.google.cloud.dataflow.sdk.runners.{DataflowPipelineRunner, DirectPipelineRunner}
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.{BigQueryClient, TableRow}
import com.spotify.scio.coders.KryoAtomicCoder
import org.apache.avro.generic.GenericRecord

import scala.collection.JavaConverters._
import scala.collection.mutable.{ListBuffer, Map => MMap}
import scala.reflect.ClassTag

trait Sink[+T] {
  def value: Iterator[T] = {
    if (_sc.pipeline.getRunner.isInstanceOf[DataflowPipelineRunner]) {
      throw new RuntimeException("Sink[T].value cannot be used with DataflowPipelineRunner.")
    }
    if (!_sc.isClosed) {
      throw new RuntimeException("Sink[T].value can only be used after the context is closed.")
    }
    _value
  }
  protected val _sc: ScioContext
  protected def _value: Iterator[T]
}

private[scio] class UnsupportedSink[T](protected val _sc: ScioContext, msg: String) extends Sink[T] {
  override def _value: Iterator[T] = throw new RuntimeException(s"Unsupported sink: $msg")
}

private[scio] class BigQuerySink(protected val _sc: ScioContext, table: TableReference) extends Sink[TableRow] {
  override protected def _value: Iterator[TableRow] =
    BigQueryClient(_sc.options.getProject, _sc.options.getGcpCredential).getTableRows(table)
}

private[scio] class GenericAvroSink(protected val _sc: ScioContext, path: String) extends Sink[GenericRecord] {
  override def _value: Iterator[GenericRecord] = FileStorage(path).genericAvroFile
}

private[scio] class SpecificAvroSink[T: ClassTag](protected val _sc: ScioContext, path: String) extends Sink[T] {
  override def _value: Iterator[T] = FileStorage(path).specificAvroFile
}

private[scio] class TextSink(protected val _sc: ScioContext, path: String) extends Sink[String] {
  override def _value: Iterator[String] = FileStorage(path).textFile
}

private[scio] class KryoSink[T](protected val _sc: ScioContext, path: String) extends Sink[T] {
  override def _value: Iterator[T] = {
    val storage = FileStorage(path)
    val iterator = storage
      .textFile
      .map(CoderUtils.decodeFromBase64(KryoAtomicCoder[T], _))
      .toSeq
      .iterator
    storage.delete()
    iterator
  }
}

private[scio] class TableRowJsonSink(protected val _sc: ScioContext, path: String) extends Sink[TableRow] {
  override def _value: Iterator[TableRow] = FileStorage(path).tableRowJsonFile
}

private[scio] class InMemorySink[T](protected val _sc: ScioContext) extends Sink[T] {
  private[scio] val id: String = UUID.randomUUID().toString
  override def _value: Iterator[T] = InMemorySinkManager.get(id).iterator
}

private[scio] class InMemoryDataFlowSink[T](private val id: String) extends GSink[T] {
  override def createWriteOperation(options: PipelineOptions): WriteOperation[T, ListBuffer[Array[Byte]]] =
    new InMemoryWriteOperation(this, id)

  override def validate(options: PipelineOptions): Unit = {
    require(classOf[DirectPipelineRunner] isAssignableFrom  options.getRunner)
  }
}

private class InMemoryWriteOperation[T](private val sink: GSink[T], private val id: String)
  extends WriteOperation[T, ListBuffer[Array[Byte]]] {

  private val coder: Coder[T] = KryoAtomicCoder[T]

  override def finalize(writerResults: Iterable[ListBuffer[Array[Byte]]], options: PipelineOptions): Unit =
    writerResults.asScala.foreach { lb =>
      InMemorySinkManager.put(id, lb.map(CoderUtils.decodeFromByteArray(coder, _)))
    }
  override def initialize(options: PipelineOptions): Unit = {}
  override def getSink: GSink[T] = sink
  override def createWriter(options: PipelineOptions): Writer[T, ListBuffer[Array[Byte]]] = new InMemoryWriter(this)

  override def getWriterResultCoder: Coder[ListBuffer[Array[Byte]]] = KryoAtomicCoder[ListBuffer[Array[Byte]]]

}

private class InMemoryWriter[T](private val writeOperation: WriteOperation[T, ListBuffer[Array[Byte]]])
  extends Writer[T, ListBuffer[Array[Byte]]] {

  private val buffer: ListBuffer[Array[Byte]] = ListBuffer.empty
  private val coder: Coder[T] = KryoAtomicCoder[T]

  override def getWriteOperation: WriteOperation[T, ListBuffer[Array[Byte]]] = writeOperation
  override def write(value: T): Unit = buffer.append(CoderUtils.encodeToByteArray(coder, value))
  override def close(): ListBuffer[Array[Byte]] = buffer
  override def open(uId: String): Unit = {}

}

private object InMemorySinkManager {

  private val cache: MMap[String, ListBuffer[Any]] = MMap.empty

  def put[T](id: String, value: TraversableOnce[T]): Unit = {
    if (!cache.contains(id)) {
      cache.put(id, ListBuffer.empty)
    }
    cache(id).appendAll(value)
  }

  def put[T](id: String, value: T): Unit =
    if (!cache.contains(id)) {
      cache.put(id, ListBuffer(value))
    } else {
      cache(id).append(value)
    }

  def get[T](id: String): ListBuffer[T] = cache(id).asInstanceOf[ListBuffer[T]]

}