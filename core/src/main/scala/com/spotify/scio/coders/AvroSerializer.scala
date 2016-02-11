package com.spotify.scio.coders

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.google.common.collect.{BiMap, HashBiMap}
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.{GenericAvroCodecs, SpecificAvroCodecs}
import com.twitter.chill.KSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase

import scala.collection.mutable.{Map => MMap}
import scala.reflect.ClassTag

private class GenericAvroSerializer extends KSerializer[GenericRecord] {

  private type AvroInjection = Injection[GenericRecord, Array[Byte]]

  private lazy val cache: MMap[Int, AvroInjection] = MMap()
  private lazy val ids: BiMap[Int, Schema] = HashBiMap.create()

  private def getInjection(id: Int): AvroInjection = this.getInjection(id, ids.get(id))
  private def getInjection(id: Int, schema: Schema): AvroInjection =
    cache.getOrElseUpdate(id, GenericAvroCodecs.withSnappyCompression[GenericRecord](schema))

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
    val injection = this.getInjection(id)
    val bytes = injection(obj)

    out.writeInt(id)
    // write schema before every record in case it's not in reader serializer's cache
    out.writeString(obj.getSchema.toString)
    out.writeInt(bytes.length)
    out.writeBytes(bytes)
    out.flush()
  }

  override def read(kryo: Kryo, in: Input, cls: Class[GenericRecord]): GenericRecord = {
    val id = in.readInt()
    val injection = this.getInjection(id, new Schema.Parser().parse(in.readString()))

    val bytes = Array.ofDim[Byte](in.readInt())
    in.readBytes(bytes)
    injection.invert(bytes).get
  }

}

private class SpecificAvroSerializer[T <: SpecificRecordBase] extends KSerializer[T] {

  private type AvroInjection = Injection[T, Array[Byte]]

  private lazy val cache: MMap[Class[_], AvroInjection] = MMap()

  private def getInjection(cls: Class[T]): AvroInjection =
    cache.getOrElseUpdate(cls, SpecificAvroCodecs.withSnappyCompression[T](ClassTag(cls)))

  override def write(kser: Kryo, out: Output, obj: T): Unit = {
    val injection = this.getInjection(obj.getClass.asInstanceOf[Class[T]])
    val bytes = injection(obj)

    out.writeInt(bytes.length)
    out.writeBytes(bytes)
    out.flush()
  }

  override def read(kser: Kryo, in: Input, cls: Class[T]): T = {
    val injection = this.getInjection(cls)

    val bytes = Array.ofDim[Byte](in.readInt())
    in.readBytes(bytes)
    injection.invert(bytes).get
  }

}
