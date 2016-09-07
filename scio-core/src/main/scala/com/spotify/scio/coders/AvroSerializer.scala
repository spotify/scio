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

package com.spotify.scio.coders

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.cloud.dataflow.sdk.coders.AvroCoder
import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.google.common.collect.{BiMap, HashBiMap}
import com.twitter.chill.KSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase

import scala.collection.mutable.{Map => MMap}

private class GenericAvroSerializer extends KSerializer[GenericRecord] {

  private lazy val cache: MMap[Int, AvroCoder[GenericRecord]] = MMap()
  private lazy val ids: BiMap[Int, Schema] = HashBiMap.create()

  private def getCoder(id: Int): AvroCoder[GenericRecord] = this.getCoder(id, ids.get(id))
  private def getCoder(id: Int, schema: Schema): AvroCoder[GenericRecord] =
    cache.getOrElseUpdate(id, AvroCoder.of(schema))

  private def getId(schema: Schema): Int =
    if (ids.containsValue(schema)) {
      ids.inverse().get(schema)
    } else {
      val id: Int = ids.size()
      ids.put(id, schema)
      id
    }

  override def write(kryo: Kryo, out: Output, obj: GenericRecord): Unit = {
    val id = this.getId(obj.getSchema)
    val coder = this.getCoder(id)
    val bytes = CoderUtils.encodeToByteArray(coder, obj)

    out.writeInt(id)
    // write schema before every record in case it's not in reader serializer's cache
    out.writeString(obj.getSchema.toString)
    out.writeInt(bytes.length)
    out.writeBytes(bytes)
    out.flush()
  }

  override def read(kryo: Kryo, in: Input, cls: Class[GenericRecord]): GenericRecord = {
    val id = in.readInt()
    val coder = this.getCoder(id, new Schema.Parser().parse(in.readString()))

    val bytes = in.readBytes(in.readInt())
    CoderUtils.decodeFromByteArray(coder, bytes)
  }

}

private class SpecificAvroSerializer[T <: SpecificRecordBase] extends KSerializer[T] {

  private lazy val cache: MMap[Class[T], AvroCoder[T]] = MMap()

  private def getCoder(cls: Class[T]): AvroCoder[T] = cache.getOrElseUpdate(cls, AvroCoder.of(cls))

  override def write(kser: Kryo, out: Output, obj: T): Unit = {
    val coder = this.getCoder(obj.getClass.asInstanceOf[Class[T]])
    val bytes = CoderUtils.encodeToByteArray(coder, obj)

    out.writeInt(bytes.length)
    out.writeBytes(bytes)
    out.flush()
  }

  override def read(kser: Kryo, in: Input, cls: Class[T]): T = {
    val coder = this.getCoder(cls)
    val bytes = in.readBytes(in.readInt())
    CoderUtils.decodeFromByteArray(coder, bytes)
  }

}
