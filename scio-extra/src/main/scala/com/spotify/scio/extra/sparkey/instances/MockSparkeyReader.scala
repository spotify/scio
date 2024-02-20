/*
 * Copyright 2023 Spotify AB.
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

package com.spotify.scio.extra.sparkey.instances

import com.spotify.sparkey.SparkeyReader.Entry
import com.spotify.sparkey.{IndexHeader, LogHeader, SparkeyReader}

import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

trait MockSparkeyReader extends SparkeyReader with Serializable {
  override def getAsString(key: String): String = ???
  override def getLogHeader: LogHeader = ???
  override def getAsByteArray(key: Array[Byte]): Array[Byte] = ???
  override def getAsEntry(key: Array[Byte]): Entry = ???
  override def iterator(): java.util.Iterator[Entry] = ???
  override def close(): Unit = ???
  override def getIndexHeader: IndexHeader = ???
  override def duplicate(): SparkeyReader = ???
  override def getLoadedBytes: Long = ???
  override def getTotalBytes: Long = ???
}

trait MockEntry[K, V] extends Entry with Serializable {
  protected def k: K
  protected def v: V
  override def getKeyLength: Int = ???
  override def getKey: Array[Byte] = ???
  override def getKeyAsString: String = ???
  override def getValueLength: Long = ???
  override def getValue: Array[Byte] = ???
  override def getValueAsString: String = ???
  override def getValueAsStream: InputStream = ???
  override def getType: SparkeyReader.Type = ???
}

case class MockStringEntry(k: String, v: String) extends MockEntry[String, String] {
  override def getKey: Array[Byte] = k.getBytes(StandardCharsets.UTF_8)
  override def getKeyAsString: String = k
  override def getValue: Array[Byte] = v.getBytes(StandardCharsets.UTF_8)
  override def getValueAsString: String = v
}

case class MockStringSparkeyReader(data: Map[String, String]) extends MockSparkeyReader {
  override def getAsString(key: String): String = data.getOrElse(key, null)
  override def iterator(): java.util.Iterator[Entry] = data.iterator
    .map[Entry] { case (k, v) => MockStringEntry(k, v) }
    .asJava
}

case class MockByteArrayEntry(k: Array[Byte], v: Array[Byte])
    extends MockEntry[Array[Byte], Array[Byte]] {
  override def getKey: Array[Byte] = k
  override def getKeyAsString: String = new String(k, StandardCharsets.UTF_8)
  override def getValue: Array[Byte] = v
  override def getValueAsString: String = new String(v, StandardCharsets.UTF_8)
}

case class MockByteArraySparkeyReader(data: Map[Array[Byte], Array[Byte]])
    extends MockSparkeyReader {
  private lazy val internal = data.map { case (k, v) => (ByteBuffer.wrap(k), v) }

  override def getAsByteArray(key: Array[Byte]): Array[Byte] =
    internal.getOrElse(ByteBuffer.wrap(key), null)
  override def iterator(): java.util.Iterator[Entry] = data.iterator
    .map[Entry] { case (k, v) => MockByteArrayEntry(k, v) }
    .asJava
}
