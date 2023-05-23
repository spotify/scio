package com.spotify.scio.coders.instances.kryo

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.twitter.chill.KSerializer
import org.apache.avro.Schema

private[coders] class SchemaSerializer extends KSerializer[Schema] {
  private lazy val parser = new Schema.Parser()

  override def write(kryo: Kryo, output: Output, schema: Schema): Unit =
    output.writeString(schema.toString)

  override def read(kryo: Kryo, input: Input, tpe: Class[Schema]): Schema =
    parser.parse(input.readString())
}
