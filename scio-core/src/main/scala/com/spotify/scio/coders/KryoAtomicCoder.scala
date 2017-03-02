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

import java.io.{ByteArrayOutputStream, IOException, InputStream, OutputStream}

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
import org.apache.beam.sdk.util.VarInt
import org.apache.beam.sdk.util.common.ElementByteSizeObserver
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
      k.register(classOf[JIterableWrapper[_]], new JIterableWrapperSerializer)

      k.forSubclass[SpecificRecordBase](new SpecificAvroSerializer)
      k.forSubclass[GenericRecord](new GenericAvroSerializer)
      k.forSubclass[Message](new ProtobufSerializer)

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
      val s = new ByteArrayOutputStream()
      val output = new Output(s)
      kryo.get().writeClassAndObject(output, value)
      output.flush()

      VarInt.encode(s.size(), outStream)
      outStream.write(s.toByteArray)
    }
  }

  override def decode(inStream: InputStream, context: Context): T = {
    val o = if (context.isWholeStream) {
      kryo.get().readClassAndObject(new Input(inStream))
    } else {
      val length = VarInt.decodeInt(inStream)
      if (length < 0) {
        throw new IOException("invalid length " + length)
      }

      val value = Array.ofDim[Byte](length)
      ByteStreams.readFully(inStream, value)
      kryo.get().readClassAndObject(new Input(value))
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
