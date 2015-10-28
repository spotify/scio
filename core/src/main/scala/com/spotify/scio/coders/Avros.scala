package com.spotify.scio.coders

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter, SpecificRecord}

import scala.reflect.ClassTag

/** Utility for Avro encoding. */
object Avros {

  def genericCoder(schema: Schema): AvroCoder[GenericRecord] = new GenericAvroCoder(schema)

  def specificCoder[T <: SpecificRecord : ClassTag]: AvroCoder[T] =
    new SpecificAvroCoder[T](implicitly[ClassTag[T]].runtimeClass)

  def specificCoder[T <: SpecificRecord](cls: Class[_]): AvroCoder[T] = new SpecificAvroCoder[T](cls)

}

/** Coder for Avro objects. */
trait AvroCoder[T] extends Serializable {
  def decode(bytes: Array[Byte]): T
  def encode(record: T): Array[Byte]
}

private class GenericAvroCoder(schema: Schema) extends AvroCoder[GenericRecord] {

  private val schemaString = schema.toString

  private lazy val reader = new GenericDatumReader[GenericRecord](new Schema.Parser().parse(schemaString))
  private lazy val writer = new GenericDatumWriter[GenericRecord](new Schema.Parser().parse(schemaString))

  override def decode(bytes: Array[Byte]): GenericRecord = {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    reader.read(null, decoder)
  }

  override def encode(record: GenericRecord): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    writer.write(record, encoder)
    encoder.flush()
    out.toByteArray
  }

}

private class SpecificAvroCoder[T <: SpecificRecord](cls: Class[_]) extends AvroCoder[T] {

  private val schemaString = cls.getMethod("getClassSchema").invoke(null).asInstanceOf[Schema].toString

  private lazy val reader = new SpecificDatumReader[T](new Schema.Parser().parse(schemaString))
  private lazy val writer = new SpecificDatumWriter[T](new Schema.Parser().parse(schemaString))

  override def decode(bytes: Array[Byte]): T = {
    val decoder = DecoderFactory.get().binaryDecoder(bytes, null)
    reader.read(null.asInstanceOf[T], decoder)
  }

  override def encode(record: T): Array[Byte] = {
    val out = new ByteArrayOutputStream
    val encoder = EncoderFactory.get.binaryEncoder(out, null)
    writer.write(record, encoder)
    encoder.flush()
    out.toByteArray
  }

}
