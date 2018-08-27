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

import java.io.{EOFException, InputStream, OutputStream}
import java.nio.file.Path
import java.util.UUID

import com.esotericsoftware.kryo.KryoException
import com.esotericsoftware.kryo.io.{InputChunked, OutputChunked}
import com.google.common.io.{ByteStreams, CountingOutputStream}
import com.google.common.reflect.ClassPath
import com.google.protobuf.{ByteString, Message}
import com.spotify.scio.coders.serializers._
import com.spotify.scio.options.ScioOptions
import com.twitter.chill._
import com.twitter.chill.algebird.AlgebirdRegistrar
import com.twitter.chill.protobuf.ProtobufSerializer
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.coders.{AtomicCoder, CoderException, InstantCoder}
import org.apache.beam.sdk.io.gcp.bigquery.TableRowJsonCoder
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.apache.beam.sdk.util.VarInt
import org.apache.beam.sdk.util.common.ElementByteSizeObserver
import org.joda.time.{DateTime, LocalDate, LocalDateTime, LocalTime}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.convert.Wrappers
import scala.collection.mutable

private object KryoRegistrarLoader {
  private[this] val logger = LoggerFactory.getLogger(this.getClass)

  def load(k: Kryo): Unit = {
    logger.debug("Loading KryoRegistrars: " + registrars.mkString(", "))
    registrars.foreach(_(k))
  }

  private val registrars: Seq[IKryoRegistrar] = {
    logger.debug("Initializing KryoRegistrars")
    val classLoader = Thread.currentThread().getContextClassLoader
    ClassPath
      .from(classLoader)
      .getAllClasses
      .asScala
      .toSeq
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

object ScioKryoRegistrar {
  private val logger = LoggerFactory.getLogger(this.getClass)
}

/** serializers we've written in Scio and want to add to Kryo serialization
 * @see com.spotify.scio.coders.serializers */
private final class ScioKryoRegistrar extends IKryoRegistrar {
  import ScioKryoRegistrar.logger

  override def apply(k: Kryo): Unit = {
    logger.debug("Loading common Kryo serializers...")
    k.forClass(new CoderSerializer(InstantCoder.of()))
    k.forClass(new CoderSerializer(TableRowJsonCoder.of()))
    // Java Iterable/Collection are missing proper equality check, use custom CBF as a
    // workaround
    k.register(classOf[Wrappers.JIterableWrapper[_]],
               new JTraversableSerializer[Any, Iterable[Any]]()(new JIterableWrapperCBF[Any]))
    k.register(classOf[Wrappers.JCollectionWrapper[_]],
               new JTraversableSerializer[Any, Iterable[Any]]()(new JCollectionWrapperCBF[Any]))
    // Wrapped Java collections may have immutable implementations, i.e. Guava, treat them
    // as regular Scala collections as a workaround
    k.register(classOf[Wrappers.JListWrapper[_]],
               new JTraversableSerializer[Any, mutable.Buffer[Any]])
    k.forSubclass[SpecificRecordBase](new SpecificAvroSerializer)
    k.forSubclass[GenericRecord](new GenericAvroSerializer)
    k.forSubclass[Message](new ProtobufSerializer)
    k.forSubclass[LocalDate](new JodaLocalDateSerializer)
    k.forSubclass[LocalTime](new JodaLocalTimeSerializer)
    k.forSubclass[LocalDateTime](new JodaLocalDateTimeSerializer)
    k.forSubclass[DateTime](new JodaDateTimeSerializer)
    k.forSubclass[Path](new JPathSerializer)
    k.forSubclass[ByteString](new ByteStringSerializer)
    k.forClass(new KVSerializer)
    // TODO:
    // TimestampedValueCoder
  }
}

private[scio] final class KryoAtomicCoder[T](private val options: KryoOptions)
    extends AtomicCoder[T] {
  import KryoAtomicCoder._

  private[this] val instanceId = UUID.randomUUID.toString

  override def encode(value: T, os: OutputStream): Unit =
    withKryoState(instanceId, options) { kryoState =>
      if (value == null) {
        throw new CoderException("cannot encode a null value")
      }

      VarInt.encode(Header, os)
      val chunked = kryoState.outputChunked
      chunked.setOutputStream(os)

      try {
        kryoState.kryo.writeClassAndObject(chunked, value)
        chunked.endChunks()
        chunked.flush()
      } catch {
        case ke: KryoException =>
          // make sure that the Kryo output buffer is cleared in case that we can recover from
          // the exception (e.g. EOFException which denotes buffer full)
          chunked.clear()
          ke.getCause match {
            case ex: EOFException => throw ex
            case _                => throw ke
          }
      }
    }

  override def decode(is: InputStream): T = withKryoState(instanceId, options) { kryoState =>
    val chunked = kryoState.inputChunked
    val o = if (VarInt.decodeInt(is) == Header) {
      chunked.setInputStream(is)

      kryoState.kryo.readClassAndObject(chunked)
    } else {
      kryoState.kryo.readClassAndObject(new Input(chunked.getBuffer))
    }
    o.asInstanceOf[T]
  }

  // This method is called by PipelineRunner to sample elements in a PCollection and estimate
  // size. This could be expensive for collections with small number of very large elements.
  override def registerByteSizeObserver(value: T, observer: ElementByteSizeObserver): Unit =
    value match {
      // (K, Iterable[V]) is the return type of `groupBy` or `groupByKey`. This could be very slow
      // when there're few keys with many values.
      case (key, wrapper: Wrappers.JIterableWrapper[_]) =>
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
            logger.warn(
              s"Aborting size estimation for ${wrapper.underlying.getClass}, " +
                s"elapsed: $elapsed ms, count: $count, bytes: $bytes")
            wrapper.underlying match {
              case c: _root_.java.util.Collection[_] =>
                // extrapolate remaining bytes in the collection
                val remaining = (bytes.toDouble / count * (c.size - count)).toLong
                observer.update(remaining)
                logger.warn(
                  s"Extrapolated size estimation for ${wrapper.underlying.getClass} " +
                    s"count: ${c.size}, bytes: ${bytes + remaining}")
              case _ =>
                logger.warn("Can't get size of internal collection, thus can't extrapolate size")
            }
          } else if (elapsed > warningThreshold && !warned) {
            warned = true
            logger.warn(
              s"Slow size estimation for ${wrapper.underlying.getClass}, " +
                s"elapsed: $elapsed ms, count: $count, bytes: $bytes")
          }
        }
      case _ =>
        observer.update(kryoEncodedElementByteSize(value))
    }

  private def kryoEncodedElementByteSize(obj: Any): Long = withKryoState(instanceId, options) {
    kryoState: KryoState =>
      val s = new CountingOutputStream(ByteStreams.nullOutputStream())
      val output = new Output(options.bufferSize, options.maxBufferSize)
      output.setOutputStream(s)
      kryoState.kryo.writeClassAndObject(output, obj)
      output.flush()
      s.getCount + VarInt.getLength(s.getCount)
  }

}

/** Used for sharing Kryo instance and buffers. */
private[scio] final case class KryoState(kryo: Kryo,
                                         inputChunked: InputChunked,
                                         outputChunked: OutputChunked)

private[scio] object KryoAtomicCoder {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val Header = -1

  // We want to have one Kryo instance per thread per instance.
  // Also the instances should be garbage collected when the thread dies.
  private[this] val KryoStateMap: ThreadLocal[mutable.HashMap[String, KryoState]] =
    new ThreadLocal[mutable.HashMap[String, KryoState]] {
      override def initialValue(): mutable.HashMap[String, KryoState] =
        mutable.HashMap[String, KryoState]()
    }

  final def withKryoState[R](instanceId: String, options: KryoOptions)(f: KryoState => R): R = {
    val ks = KryoStateMap.get().getOrElseUpdate(instanceId, {
      val k = KryoSerializer.registered.newKryo()
      k.setReferences(options.referenceTracking)
      k.setRegistrationRequired(options.registrationRequired)

      new ScioKryoRegistrar()(k)
      new AlgebirdRegistrar()(k)

      KryoRegistrarLoader.load(k)

      val input = new InputChunked(options.bufferSize)
      val output = new OutputChunked(options.bufferSize)

      KryoState(k, input, output)
    })

    f(ks)
  }
}

private[scio] final case class KryoOptions(bufferSize: Int,
                                           maxBufferSize: Int,
                                           referenceTracking: Boolean,
                                           registrationRequired: Boolean)

private[scio] object KryoOptions {
  @inline def apply(): KryoOptions = KryoOptions(PipelineOptionsFactory.create())

  def apply(options: PipelineOptions): KryoOptions = {
    val o = options.as(classOf[ScioOptions])
    KryoOptions(o.getKryoBufferSize,
                o.getKryoMaxBufferSize,
                o.getKryoReferenceTracking,
                o.getKryoRegistrationRequired)
  }
}
