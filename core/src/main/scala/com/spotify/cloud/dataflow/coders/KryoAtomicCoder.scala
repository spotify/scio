package com.spotify.cloud.dataflow.coders

import java.io.{IOException, ByteArrayOutputStream, OutputStream, InputStream}

import com.google.cloud.dataflow.sdk.coders.{CoderException, AtomicCoder}
import com.google.cloud.dataflow.sdk.coders.Coder.Context
import com.google.cloud.dataflow.sdk.util.VarInt
import com.google.common.io.ByteStreams
import com.twitter.chill._
import org.apache.avro.specific.SpecificRecord

// TODO: need to fix for scala 2.11
import scala.collection.JavaConversions.JIterableWrapper

private[dataflow] class KryoAtomicCoder extends AtomicCoder[Any] {

  @transient
  private lazy val kryo: Kryo = {
    val k = KryoSerializer.registered.newKryo()

    // java.lang.Iteable.asScala returns JIterableWrapper which causes problem.
    // Treat it as standard Iteable instead.
    // TODO: need to fix for scala 2.11
    k.register(classOf[JIterableWrapper[_]], new JIterableWrapperSerializer)

    k.forSubclass[SpecificRecord](new AvroSerializer)

    k.forClass(new KVSerializer)
    // TODO:
    // InstantCoder
    // TimestampedValueCoder

    k
  }

  override def encode(value: Any, outStream: OutputStream, context: Context): Unit = {
    if (value == null) {
      throw new CoderException("cannot encode a null value")
    }
    if (context.isWholeStream) {
      val output = new Output(outStream)
      kryo.writeClassAndObject(output, value)
      output.flush()
    } else {
      val s = new ByteArrayOutputStream()
      val output = new Output(s)
      kryo.writeClassAndObject(output, value)
      output.flush()
      s.close()

      VarInt.encode(s.size(), outStream)
      outStream.write(s.toByteArray)
    }
  }

  override def decode(inStream: InputStream, context: Context): Any = {
    if (context.isWholeStream) {
      kryo.readClassAndObject(new Input(inStream))
    } else {
      val length = VarInt.decodeInt(inStream)
      if (length < 0) {
        throw new IOException("invalid length " + length)
      }

      val value = Array.ofDim[Byte](length)
      ByteStreams.readFully(inStream, value)
      kryo.readClassAndObject(new Input(value))
    }
  }

  // TODO: double check
  override def isDeterministic: Boolean = true

}
