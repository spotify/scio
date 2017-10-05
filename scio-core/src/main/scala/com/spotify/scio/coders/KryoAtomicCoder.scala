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

import java.io.{InputStream, OutputStream}

import com.esotericsoftware.kryo.io.{InputChunked, OutputChunked}
import com.google.common.io.{ByteStreams, CountingOutputStream}
import com.google.common.reflect.ClassPath
import com.google.protobuf.Message
import com.twitter.chill._
import com.twitter.chill.algebird.AlgebirdRegistrar
import com.twitter.chill.protobuf.ProtobufSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.coders._
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder
import org.apache.beam.sdk.util.{EmptyOnDeserializationThreadLocal, VarInt}
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
  private val bufferSize = 64 * 1024

  import KryoAtomicCoder.logger

  private val kryoState: ThreadLocal[KryoState] =
    new EmptyOnDeserializationThreadLocal[KryoState] {
      override def initialValue(): KryoState = {
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

        val input = new InputChunked(bufferSize)
        val output = new OutputChunked(bufferSize)

        KryoState(k, input, output)
      }
    }

  private val header = -1

  override def encode(value: T, os: OutputStream): Unit = {
    if (value == null) {
      throw new CoderException("cannot encode a null value")
    }

    VarInt.encode(header, os)

    val state = kryoState.get()

    val chunked = state.output
    chunked.setOutputStream(os)

    state.kryo.writeClassAndObject(chunked, value)
    chunked.endChunks()
    chunked.flush()
  }

  override def decode(is: InputStream): T = {
    val state = kryoState.get()

    val o = if (VarInt.decodeInt(is) == header) {
      val chunked = state.input
      chunked.setInputStream(is)

      state.kryo.readClassAndObject(chunked)
    } else {
      state.kryo.readClassAndObject(new Input(state.input.getBuffer))
    }

    o.asInstanceOf[T]
  }

  // This method is called by PipelineRunner to sample elements in a PCollection and estimate
  // size. This could be expensive for collections with small number of very large elements.
  override def registerByteSizeObserver(value: T,
                                        observer: ElementByteSizeObserver): Unit = value match {
    // (K, Iterable[V]) is the return type of `groupBy` or `groupByKey`. This could be very slow
    // when there're few keys with many values.
    case (key, wrapper: JIterableWrapper[_]) =>
      observer.update(kryoEncodedElementByteSize(key))
      // FIXME: handle ElementByteSizeObservableIterable[_, _]
      var count = 0
      var bytes = 0L
      var warned = false
      var aborted = false
      val warningThreshold = 10000 // 10s
      val abortThreshold = 60000 // 1min
      val start = System.currentTimeMillis()
      val i = wrapper.underlying.iterator()
      while (i.hasNext && !aborted) {
        val size = kryoEncodedElementByteSize(i.next())
        observer.update(size)
        count += 1
        bytes += size
        val elapsed = System.currentTimeMillis() - start
        if (elapsed > abortThreshold) {
          aborted = true
          logger.warn(s"Aborting size estimation for ${wrapper.underlying.getClass}, " +
            s"elapsed: $elapsed ms, count: $count, bytes: $bytes")
          wrapper.underlying match {
            case c: _root_.java.util.Collection[_] =>
              // extrapolate remaining bytes in the collection
              val remaining = (bytes.toDouble / count * (c.size - count)).toLong
              observer.update(remaining)
              logger.warn(s"Extrapolated size estimation for ${wrapper.underlying.getClass} " +
                s"count: ${c.size}, bytes: ${bytes + remaining}")
            case _ =>
              logger.warn("Can't get size of internal collection, thus can't extrapolate size")
          }
        } else if (elapsed > warningThreshold && !warned) {
          warned = true
          logger.warn(s"Slow size estimation for ${wrapper.underlying.getClass}, " +
            s"elapsed: $elapsed ms, count: $count, bytes: $bytes")
        }
      }
    case _ =>
      observer.update(kryoEncodedElementByteSize(value))
  }

  private def kryoEncodedElementByteSize(obj: Any): Long = {
    val s = new CountingOutputStream(ByteStreams.nullOutputStream())
    val output = new Output(s)
    kryoState.get().kryo.writeClassAndObject(output, obj)
    output.flush()
    s.getCount + VarInt.getLength(s.getCount)
  }

}

/** Used for sharing Kryo instance and buffers */
private[scio] final case class KryoState(kryo: Kryo, input: InputChunked, output: OutputChunked)

private[scio] object KryoAtomicCoder {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def apply[T]: Coder[T] = new KryoAtomicCoder[T]

}
