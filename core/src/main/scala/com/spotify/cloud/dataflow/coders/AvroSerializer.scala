package com.spotify.cloud.dataflow.coders

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.common.collect.{HashBiMap, BiMap}
import com.spotify.cloud.dataflow.io.{SpecificAvroCoder, Avros, GenericAvroCoder}
import com.twitter.chill.KSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord

import scala.collection.mutable.{Map => MMap}

private class GenericAvroSerializer extends KSerializer[GenericRecord] {

  private lazy val cache: MMap[Int, GenericAvroCoder] = MMap()
  private lazy val ids: BiMap[Int, Schema] = HashBiMap.create()

  private def get(id: Int) = cache.getOrElseUpdate(id, Avros.genericCoder(ids.get(id)))
  private def get(id: Int, schema: Schema) = cache.getOrElseUpdate(id, Avros.genericCoder(schema))

  private def getId(schema: Schema): Int =
    if (ids.containsValue(schema)) {
      ids.inverse().get(schema)
    } else {
      val id: Int = ids.size()
      ids.put(id, schema)
      id
    }

  override def write(kryo: Kryo, out: Output, obj: GenericRecord): Unit = {
    val id = getId(obj.getSchema)
    val coder = get(id)
    val bytes = coder.encode(obj)

    out.writeInt(id)
    // write schema before every record in case it's not in reader serializer's cache
    out.writeString(obj.getSchema.toString)
    out.writeInt(bytes.length)
    out.writeBytes(bytes)
    out.flush()
  }

  override def read(kryo: Kryo, in: Input, cls: Class[GenericRecord]): GenericRecord = {
    val id = in.readInt()
    val coder = get(id, new Schema.Parser().parse(in.readString()))

    val bytes = Array.ofDim[Byte](in.readInt())
    in.readBytes(bytes)
    coder.decode(bytes)
  }

}

private class SpecificAvroSerializer[T <: SpecificRecord] extends KSerializer[T] {

  private lazy val cache: MMap[Class[_], SpecificAvroCoder[_]] = MMap()

  private def get(cls: Class[T]) =
    cache.getOrElseUpdate(cls, Avros.specificCoder(cls)).asInstanceOf[SpecificAvroCoder[T]]

  override def write(kser: Kryo, out: Output, obj: T): Unit = {
    val coder = get(obj.getClass.asInstanceOf[Class[T]])
    val bytes = coder.encode(obj)

    out.writeInt(bytes.length)
    out.writeBytes(bytes)
    out.flush()
  }

  override def read(kser: Kryo, in: Input, cls: Class[T]): T = {
    val coder = get(cls)

    val bytes = Array.ofDim[Byte](in.readInt())
    in.readBytes(bytes)
    coder.decode(bytes)
  }

}
