package com.spotify.scio.coders

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.common.collect.{BiMap, HashBiMap}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.{GenericAvroCodecs, SpecificAvroCodec}
import com.twitter.chill.KSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase

import scala.collection.mutable.{Map => MMap}

private class GenericAvroSerializer extends KSerializer[GenericRecord] {

  private lazy val cache: MMap[Int, Injection[GenericRecord, Array[Byte]]] = MMap()
  private lazy val ids: BiMap[Int, Schema] = HashBiMap.create()

  private def get(id: Int) = cache.getOrElseUpdate(id, GenericAvroCodecs[GenericRecord](ids.get(id)))
  private def get(id: Int, schema: Schema) = cache.getOrElseUpdate(id, GenericAvroCodecs[GenericRecord](schema))

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
    val coder = this.get(id)
    val bytes = coder(obj)

    out.writeInt(id)
    // write schema before every record in case it's not in reader serializer's cache
    out.writeString(obj.getSchema.toString)
    out.writeInt(bytes.length)
    out.writeBytes(bytes)
    out.flush()
  }

  override def read(kryo: Kryo, in: Input, cls: Class[GenericRecord]): GenericRecord = {
    val id = in.readInt()
    val coder = this.get(id, new Schema.Parser().parse(in.readString()))

    val bytes = Array.ofDim[Byte](in.readInt())
    in.readBytes(bytes)
    coder.invert(bytes).get
  }

}

private class SpecificAvroSerializer[T <: SpecificRecordBase] extends KSerializer[T] {

  private lazy val cache: MMap[Class[_], Injection[T, Array[Byte]]] = MMap()

  private def get(cls: Class[T]) =
    cache.getOrElseUpdate(cls, new SpecificAvroCodec[T](cls))

  override def write(kser: Kryo, out: Output, obj: T): Unit = {
    val coder = this.get(obj.getClass.asInstanceOf[Class[T]])
    val bytes = coder(obj)

    out.writeInt(bytes.length)
    out.writeBytes(bytes)
    out.flush()
  }

  override def read(kser: Kryo, in: Input, cls: Class[T]): T = {
    val coder = this.get(cls)

    val bytes = Array.ofDim[Byte](in.readInt())
    in.readBytes(bytes)
    coder.invert(bytes).get
  }

}
