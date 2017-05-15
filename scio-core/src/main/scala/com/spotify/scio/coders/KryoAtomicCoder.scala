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

import java.io.{ByteArrayInputStream, InputStream, OutputStream}
import java.nio.ByteBuffer

import com.google.common.io.{ByteStreams, CountingOutputStream}
import com.google.common.reflect.ClassPath
import com.google.protobuf.Message
import com.twitter.chill._
import com.twitter.chill.algebird.AlgebirdRegistrar
import com.twitter.chill.protobuf.ProtobufSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.coders.Coder.Context
import org.apache.beam.sdk.coders._
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder
import org.apache.beam.sdk.util.VarInt
import org.apache.beam.sdk.util.common.ElementByteSizeObserver
import org.joda.time.{LocalDate, LocalDateTime}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.convert.Wrappers.JIterableWrapper

private object KryoRegistrarLoader {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def load(k: Kryo): Unit = {
    logger.debug("Loading KryoRegistrars: " + registrars.mkString(", "))
    registrars.foreach(_(k))
  }

  private val registrars: Seq[IKryoRegistrar] = {
    logger.debug("Initializing KryoRegistrars")
    val classLoader = Thread.currentThread().getContextClassLoader
    ClassPath.from(classLoader).getAllClasses.asScala.toSeq
      .filter(_.getName.endsWith("KryoRegistrar"))
      .flatMap { clsInfo =>
        val optCls: Option[IKryoRegistrar] = try {
          val cls = clsInfo.load()
          if (classOf[AnnotatedKryoRegistrar] isAssignableFrom cls) {
            Some(cls.newInstance().asInstanceOf[IKryoRegistrar])
          } else {
            None
          }
        } catch {
          case _: Throwable => None
        }
        optCls
      }
  }

}

private[scio] class KryoAtomicCoder[T] extends AtomicCoder[T] {

  @transient
  private lazy val kryo: ThreadLocal[Kryo] = new ThreadLocal[Kryo] {
    override def initialValue(): Kryo = {
      val k = KryoSerializer.registered.newKryo()

      k.forClass(new CoderSerializer(InstantCoder.of()))
      k.forClass(new CoderSerializer(TableRowJsonCoder.of()))

      // java.lang.Iterable.asScala returns JIterableWrapper which causes problem.
      // Treat it as standard Iterable instead.
      k.register(classOf[JIterableWrapper[_]], new JIterableWrapperSerializer())

      k.forSubclass[SpecificRecordBase](new SpecificAvroSerializer)
      k.forSubclass[GenericRecord](new GenericAvroSerializer)
      k.forSubclass[Message](new ProtobufSerializer)

      k.forSubclass[LocalDateTime](new JodaLocalDateTimeSerializer)
      k.forSubclass[LocalDate](new JodaLocalDateSerializer)

      k.forClass(new KVSerializer)
      // TODO:
      // TimestampedValueCoder

      new AlgebirdRegistrar()(k)
      KryoRegistrarLoader.load(k)

      k
    }
  }

  override def encode(value: T, outStream: OutputStream, context: Context): Unit = {
    if (value == null) {
      throw new CoderException("cannot encode a null value")
    }
    if (context.isWholeStream) {
      val output = new Output(outStream)
      kryo.get().writeClassAndObject(output, value)
      output.flush()
    } else {
      val os = new BufferedPrefixOutputStream(outStream)
      val output = new Output(os)
      kryo.get().writeClassAndObject(output, value)
      output.flush()
      os.finish()
    }
  }

  override def decode(inStream: InputStream, context: Context): T = {
    val o = if (context.isWholeStream) {
      kryo.get().readClassAndObject(new Input(inStream))
    } else {
      val is = new BufferedPrefixInputStream(inStream)
      val obj = kryo.get().readClassAndObject(new Input(is))
      is.finish()
      obj
    }
    o.asInstanceOf[T]
  }

  // This method is called by PipelineRunner to sample elements in a PCollection and estimate
  // size. This could be expensive for collections with small number of very large elements.
  override def registerByteSizeObserver(value: T, observer: ElementByteSizeObserver,
                                        context: Context): Unit = value match {
    // (K, Iterable[V]) is the return type of `groupBy` or `groupByKey`. This could be very slow
    // when there're few keys with many values.
    case (key, wrapper: JIterableWrapper[_]) =>
      observer.update(kryoEncodedElementByteSize(key, Context.OUTER))
      // FIXME: handle ElementByteSizeObservableIterable[_, _]
      val i = wrapper.underlying.iterator()
      while (i.hasNext) {
        observer.update(kryoEncodedElementByteSize(i.next(), Context.OUTER))
      }
    case _ =>
      observer.update(kryoEncodedElementByteSize(value, context))
  }

  private def kryoEncodedElementByteSize(obj: Any, context: Context): Long = {
    val s = new CountingOutputStream(ByteStreams.nullOutputStream())
    val output = new Output(s)
    kryo.get().writeClassAndObject(output, obj)
    output.flush()
    if (context.isWholeStream) s.getCount else s.getCount + VarInt.getLength(s.getCount)
  }

}

private[scio] object KryoAtomicCoder {
  def apply[T]: Coder[T] = new KryoAtomicCoder[T]
}

/**
 * Buffered output stream that adds length prefix to each buffer block. Useful for large objects
 * or collections. Based on [[org.apache.beam.sdk.util.BufferedElementCountingOutputStream]].
 */
private class BufferedPrefixOutputStream(private val os: OutputStream)
  extends OutputStream {

  private val buffer = ByteBuffer.allocate(64 * 1024)
  private var finished = false

  VarInt.encode(-1, os)

  override def write(b: Int): Unit = {
    require(!finished, "Stream has been finished. Can not add any more data.")
    if (!buffer.hasRemaining) {
      outputBuffer()
    }
    buffer.put(b.toByte)
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    require(!finished, "Stream has been finished. Can not add any more data.")
    if (buffer.remaining() >= len) {
      buffer.put(b, off, len)
    } else {
      outputBuffer()
      if (len < buffer.capacity()) {
        buffer.put(b, off, len)
      } else {
        VarInt.encode(len, os)
        os.write(b, off, len)
      }
    }
  }

  override def flush(): Unit = if (!finished) {
    outputBuffer()
    os.flush()
  }

  def finish(): Unit = if (!finished) {
    flush()
    VarInt.encode(0, os)
    finished = true
  }

  private def outputBuffer(): Unit = if (buffer.position() > 0) {
    VarInt.encode(buffer.position(), os)
    os.write(buffer.array(), buffer.arrayOffset(), buffer.position())
    buffer.clear()
  }

}

/** Counterpart for [[BufferedPrefixOutputStream]]. */
private class BufferedPrefixInputStream(private val is: InputStream) extends InputStream {

  require(VarInt.decodeInt(is) == -1, "Invalid input stream")
  inputBuffer()

  private var buffer: Array[Byte] = _
  private var bais: ByteArrayInputStream = _
  private var finished = false

  override def read(): Int = {
    inputBuffer()
    bais.read()
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    var bytesRead = 0
    var n = 0
    do {
      inputBuffer()
      n = bais.read(b, off + bytesRead, len - bytesRead)
      if (n > 0) {
        bytesRead += n
      }
    } while (n > 0)
    bytesRead
  }

  private def inputBuffer(): Unit = if (!finished) {
    if (bais == null || bais.available() <= 0) {
      val len = VarInt.decodeInt(is)
      if (len == 0) {
        finished = true
      } else {
        if (buffer == null || buffer.length < len) {
          buffer = Array.ofDim[Byte](math.max(len, 64 * 1024))
        }
        is.read(buffer, 0, len)
        bais = new ByteArrayInputStream(buffer, 0, len)
      }
    }
  }

  def finish(): Unit = if (!finished) {
    require(VarInt.decodeInt(is) == 0, "Invalid end of input stream")
  }

}
