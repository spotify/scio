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

package com.spotify.scio.io

import java.lang.Iterable

import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.io.Sink
import com.google.cloud.dataflow.sdk.io.Sink.{WriteOperation, Writer}
import com.google.cloud.dataflow.sdk.options.PipelineOptions
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.spotify.scio.coders.KryoAtomicCoder
import com.spotify.scio.util.ScioUtil

import scala.collection.JavaConverters._
import scala.collection.mutable.{Buffer => MBuffer, Map => MMap}

private[scio] class InMemorySink[T](private val id: String) extends Sink[T] {
  override def createWriteOperation(options: PipelineOptions)
  : WriteOperation[T, MBuffer[Array[Byte]]] =
    new InMemoryWriteOperation(this, id)

  override def validate(options: PipelineOptions): Unit = {
    require(ScioUtil.isLocalRunner(options),
      "InMemoryDataFlowSink can only be used with InProcessPipelineRunner or DirectPipelineRunner")
  }
}

private class InMemoryWriteOperation[T](private val sink: Sink[T], private val id: String)
  extends WriteOperation[T, MBuffer[Array[Byte]]] {

  private val coder: Coder[T] = KryoAtomicCoder[T]

  override def finalize(writerResults: Iterable[MBuffer[Array[Byte]]],
                        options: PipelineOptions): Unit =
    writerResults.asScala.foreach { lb =>
      InMemorySinkManager.put(id, lb.map(CoderUtils.decodeFromByteArray(coder, _)))
    }
  override def initialize(options: PipelineOptions): Unit = {}
  override def getSink: Sink[T] = sink
  override def createWriter(options: PipelineOptions): Writer[T, MBuffer[Array[Byte]]] =
    new InMemoryWriter(this)

  override def getWriterResultCoder: Coder[MBuffer[Array[Byte]]] =
    KryoAtomicCoder[MBuffer[Array[Byte]]]

}

private class InMemoryWriter[T](private val writeOperation: WriteOperation[T, MBuffer[Array[Byte]]])
  extends Writer[T, MBuffer[Array[Byte]]] {

  private val buffer: MBuffer[Array[Byte]] = MBuffer.empty
  private val coder: Coder[T] = KryoAtomicCoder[T]

  override def getWriteOperation: WriteOperation[T, MBuffer[Array[Byte]]] = writeOperation
  override def write(value: T): Unit = buffer.append(CoderUtils.encodeToByteArray(coder, value))
  override def close(): MBuffer[Array[Byte]] = buffer
  override def open(uId: String): Unit = {}

}

private[scio] object InMemorySinkManager {

  private val cache: MMap[String, MBuffer[Any]] = MMap.empty

  def put[T](id: String, value: TraversableOnce[T]): Unit = {
    if (!cache.contains(id)) {
      cache.put(id, MBuffer.empty)
    }
    cache(id).appendAll(value)
  }

  def put[T](id: String, value: T): Unit =
    if (!cache.contains(id)) {
      cache.put(id, MBuffer(value))
    } else {
      cache(id).append(value)
    }

  def get[T](id: String): MBuffer[T] = cache(id).asInstanceOf[MBuffer[T]]

}
