/*
 * Copyright 2020 Spotify AB
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

package com.spotify.scio.extra.sparkey.instances

import java.util

import com.spotify.sparkey.{IndexHeader, LogHeader, SparkeyReader}

import scala.util.hashing.MurmurHash3
import scala.jdk.CollectionConverters._

/**
 * A wrapper class around SparkeyReader that allows the reading of multiple Sparkey files, sharded
 * by their keys (via MurmurHash3). At most 32,768 Sparkey files are supported.
 *
 * @param sparkeys
 *   a map of shard ID to sparkey reader
 * @param numShards
 *   the total count of shards used (needed for keying as some shards may be empty)
 */
class ShardedSparkeyReader(val sparkeys: Map[Short, SparkeyReader], val numShards: Short)
    extends SparkeyReader {
  def hashKey(arr: Array[Byte]): Short =
    Math.floorMod(MurmurHash3.bytesHash(arr, 1), numShards.toInt).toShort

  def hashKey(str: String): Short =
    Math.floorMod(MurmurHash3.stringHash(str, 1), numShards.toInt).toShort

  override def getAsString(key: String): String = {
    val hashed = hashKey(key)
    if (sparkeys.contains(hashed)) {
      sparkeys(hashed).getAsString(key)
    } else {
      null
    }
  }

  override def getAsByteArray(key: Array[Byte]): Array[Byte] = {
    val hashed = hashKey(key)
    if (sparkeys.contains(hashed)) {
      sparkeys(hashed).getAsByteArray(key)
    } else {
      null
    }
  }

  override def getAsEntry(key: Array[Byte]): SparkeyReader.Entry = {
    val hashed = hashKey(key)
    if (sparkeys.contains(hashed)) {
      sparkeys(hashed).getAsEntry(key)
    } else {
      null
    }
  }

  override def getIndexHeader: IndexHeader =
    throw new NotImplementedError("ShardedSparkeyReader does not support getIndexHeader.")

  override def getLogHeader: LogHeader =
    throw new NotImplementedError("ShardedSparkeyReader does not support getLogHeader.")

  override def duplicate(): SparkeyReader =
    new ShardedSparkeyReader(sparkeys.map { case (k, v) => (k, v.duplicate) }, numShards)

  override def close(): Unit = sparkeys.values.foreach(_.close())

  override def iterator(): util.Iterator[SparkeyReader.Entry] =
    sparkeys.values.map(_.iterator.asScala).reduce(_ ++ _).asJava

  override def getLoadedBytes: Long = sparkeys.valuesIterator.map(_.getLoadedBytes).sum

  override def getTotalBytes: Long = sparkeys.valuesIterator.map(_.getTotalBytes).sum
}
