/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.extra.sparkey

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.extra.sparkey.instances.{SparkeyMap, SparkeySet, SparkeyWriter}
import com.spotify.scio.util.RemoteFileUtil
import com.spotify.scio.values.SideInput
import com.spotify.sparkey.SparkeyReader
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.PCollectionView
import org.slf4j.LoggerFactory

import scala.util.hashing.MurmurHash3

object Sparkey {
  @transient private[sparkey] lazy val logger = LoggerFactory.getLogger(this.getClass)
}

private[sparkey] class SparkeySideInput(val view: PCollectionView[SparkeyUri])
    extends SideInput[SparkeyReader] {
  override def updateCacheOnGlobalWindow: Boolean = false
  override def get[I, O](context: DoFn[I, O]#ProcessContext): SparkeyReader =
    SparkeySideInput.checkMemory(
      context.sideInput(view).getReader(RemoteFileUtil.create(context.getPipelineOptions))
    )
}

/**
 * A Sparkey-backed MapSideInput, named "Large" to help discovery and usability. For most
 * MapSideInput use cases >100MB or so, this performs dramatically faster.(100-1000x)
 */
private[sparkey] class LargeMapSideInput[K: Coder, V: Coder](val view: PCollectionView[SparkeyUri])
    extends SideInput[SparkeyMap[K, V]] {
  override def updateCacheOnGlobalWindow: Boolean = false
  override def get[I, O](context: DoFn[I, O]#ProcessContext): SparkeyMap[K, V] = {
    new SparkeyMap(
      context.sideInput(view).getReader(RemoteFileUtil.create(context.getPipelineOptions)),
      CoderMaterializer.beam(context.getPipelineOptions, Coder[K]),
      CoderMaterializer.beam(context.getPipelineOptions, Coder[V])
    )
  }
}

/**
 * A Sparkey-backed SetSideInput, named as such to help discovery and usability. For most
 * SetSideInput use cases >100MB or so, this performs dramatically faster.(100-1000x)
 */
private[sparkey] class LargeSetSideInput[K: Coder](val view: PCollectionView[SparkeyUri])
    extends SideInput[SparkeySet[K]] {
  override def updateCacheOnGlobalWindow: Boolean = false
  override def get[I, O](context: DoFn[I, O]#ProcessContext): SparkeySet[K] =
    new SparkeySet(
      context.sideInput(view).getReader(RemoteFileUtil.create(context.getPipelineOptions)),
      CoderMaterializer.beam(context.getPipelineOptions, Coder[K])
    )
}

private[sparkey] object SparkeySideInput {
  @transient private lazy val logger = LoggerFactory.getLogger(this.getClass)
  def checkMemory(reader: SparkeyReader): SparkeyReader = {
    val memoryBytes = java.lang.management.ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[com.sun.management.OperatingSystemMXBean]
      .getTotalPhysicalMemorySize
    if (reader.getTotalBytes > memoryBytes) {
      logger.warn(
        "Sparkey size {} > total memory {}, look up performance will be severely degraded. " +
          "Increase memory or use faster SSD drives.",
        reader.getTotalBytes,
        memoryBytes
      )
    }
    reader
  }
}

sealed trait SparkeyWritable[K, V] extends Serializable {
  private[sparkey] def put(w: SparkeyWriter, key: K, value: V): Unit
  private[sparkey] def shardHash(key: K): Int
}

trait WritableInstances {
  implicit val stringSparkeyWritable: SparkeyWritable[String, String] =
    new SparkeyWritable[String, String] {
      def put(w: SparkeyWriter, key: String, value: String): Unit =
        w.put(key, value)

      def shardHash(key: String): Int = MurmurHash3.stringHash(key, 1)
    }

  implicit val ByteArraySparkeyWritable: SparkeyWritable[Array[Byte], Array[Byte]] =
    new SparkeyWritable[Array[Byte], Array[Byte]] {
      def put(w: SparkeyWriter, key: Array[Byte], value: Array[Byte]): Unit =
        w.put(key, value)

      def shardHash(key: Array[Byte]): Int = MurmurHash3.bytesHash(key, 1)
    }
}
