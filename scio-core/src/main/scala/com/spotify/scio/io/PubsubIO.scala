/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.io

import com.google.protobuf.Message
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.util.{Functions, JMapWrapper, ScioUtil}
import com.spotify.scio.values.SCollection
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessageWithAttributesCoder
import org.apache.beam.sdk.io.gcp.{pubsub => beam}
import org.apache.beam.sdk.transforms.{PTransform, ParDo}
import org.apache.beam.sdk.util.CoderUtils
import org.apache.beam.sdk.values.{PCollection, PDone}
import org.joda.time.Instant

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import com.spotify.scio.io.PubsubIO.Subscription
import com.spotify.scio.io.PubsubIO.Topic

sealed trait PubsubIO[T] extends ScioIO[T] {
  override type ReadP = PubsubIO.ReadParam
  override type WriteP = PubsubIO.WriteParam
  override final val tapT = EmptyTapOf[T]

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}

object PubsubIO {
  sealed trait ReadType
  case object Subscription extends ReadType
  case object Topic extends ReadType

  final case class ReadParam(readType: ReadType) {
    val isSubscription = readType match {
      case Subscription => true
      case _            => false
    }
  }
  object ReadParam {
    // required for back compatibility
    def apply(isSubscription: Boolean): ReadParam =
      if (isSubscription) ReadParam(Subscription) else ReadParam(Topic)
  }

  final case class WriteParam(
    maxBatchSize: Option[Int] = None,
    maxBatchBytesSize: Option[Int] = None
  )

  // The following method is unsafe and exists to preserve compatibility with
  // the previous implementation while the underlying `PubsubIO` implementations
  // have been refactored and are more type safe.
  @deprecated(
    "Use readString, readAvro, readProto, readPubsub or readCoder instead.",
    since = "0.8.0"
  )
  def apply[T: ClassTag: Coder](
    name: String,
    idAttribute: String = null,
    timestampAttribute: String = null
  ): PubsubIO[T] = ScioUtil.classOf[T] match {
    case cls if classOf[String] isAssignableFrom cls =>
      StringPubsubIOWithoutAttributes(name, idAttribute, timestampAttribute)
        .asInstanceOf[PubsubIO[T]]
    case cls if classOf[SpecificRecordBase] isAssignableFrom cls =>
      type X = T with SpecificRecordBase
      AvroPubsubIOWithoutAttributes[X](name, idAttribute, timestampAttribute)(
        cls.asInstanceOf[ClassTag[X]]
      ).asInstanceOf[PubsubIO[T]]
    case cls if classOf[Message] isAssignableFrom cls =>
      type X = T with Message
      MessagePubsubIOWithoutAttributes[X](name, idAttribute, timestampAttribute)(
        cls.asInstanceOf[ClassTag[X]]
      ).asInstanceOf[PubsubIO[T]]
    case cls if classOf[beam.PubsubMessage] isAssignableFrom cls =>
      type X = T with beam.PubsubMessage
      PubSubMessagePubsubIOWithoutAttributes[X](name, idAttribute, timestampAttribute)(
        cls.asInstanceOf[ClassTag[X]]
      ).asInstanceOf[PubsubIO[T]]
    case _ =>
      FallbackPubsubIOWithoutAttributes[T](name, idAttribute, timestampAttribute)
  }

  def readString(
    name: String,
    idAttribute: String = null,
    timestampAttribute: String = null
  ): PubsubIO[String] =
    StringPubsubIOWithoutAttributes(name, idAttribute, timestampAttribute)

  def readAvro[T <: SpecificRecordBase: ClassTag](
    name: String,
    idAttribute: String = null,
    timestampAttribute: String = null
  ): PubsubIO[T] =
    AvroPubsubIOWithoutAttributes[T](name, idAttribute, timestampAttribute)

  def readProto[T <: Message: ClassTag](
    name: String,
    idAttribute: String = null,
    timestampAttribute: String = null
  ): PubsubIO[T] =
    MessagePubsubIOWithoutAttributes[T](name, idAttribute, timestampAttribute)

  def readPubsub[T <: beam.PubsubMessage: ClassTag](
    name: String,
    idAttribute: String = null,
    timestampAttribute: String = null
  ): PubsubIO[T] =
    PubSubMessagePubsubIOWithoutAttributes[T](name, idAttribute, timestampAttribute)

  def readCoder[T: Coder: ClassTag](
    name: String,
    idAttribute: String = null,
    timestampAttribute: String = null
  ): PubsubIO[T] =
    FallbackPubsubIOWithoutAttributes[T](name, idAttribute, timestampAttribute)

  def withAttributes[T: ClassTag: Coder](
    name: String,
    idAttribute: String = null,
    timestampAttribute: String = null
  ): PubsubIO[(T, Map[String, String])] =
    PubsubIOWithAttributes[T](name, idAttribute, timestampAttribute)

  private[io] def setAttrs[T](
    r: beam.PubsubIO.Read[T]
  )(idAttribute: String, timestampAttribute: String): beam.PubsubIO.Read[T] = {
    val r0 = Option(idAttribute)
      .map { att =>
        r.withIdAttribute(att)
      }
      .getOrElse(r)

    Option(timestampAttribute)
      .map { att =>
        r0.withTimestampAttribute(att)
      }
      .getOrElse(r0)
  }

  private[io] def setAttrs[T](
    r: beam.PubsubIO.Write[T]
  )(idAttribute: String, timestampAttribute: String): beam.PubsubIO.Write[T] = {
    val r0 = Option(idAttribute)
      .map { att =>
        r.withIdAttribute(att)
      }
      .getOrElse(r)

    Option(timestampAttribute)
      .map { att =>
        r0.withTimestampAttribute(att)
      }
      .getOrElse(r0)
  }
}

private sealed trait PubsubIOWithoutAttributes[T] extends PubsubIO[T] {
  def name: String
  def idAttribute: String
  def timestampAttribute: String

  override def testId: String =
    s"PubsubIO($name, $idAttribute, $timestampAttribute)"

  protected def setup[U](
    read: beam.PubsubIO.Read[U],
    params: PubsubIO.ReadParam
  ): beam.PubsubIO.Read[U] = {
    val r =
      params.readType match {
        case Subscription => read.fromSubscription(name)
        case Topic        => read.fromTopic(name)
      }

    PubsubIO.setAttrs(r)(idAttribute, timestampAttribute)
  }

  protected def setup[U](write: beam.PubsubIO.Write[U], params: PubsubIO.WriteParam) = {
    val w = PubsubIO.setAttrs(write.to(name))(idAttribute, timestampAttribute)
    params.maxBatchBytesSize.foreach(w.withMaxBatchBytesSize)
    params.maxBatchSize.foreach(w.withMaxBatchSize)
    w
  }
}

private final case class StringPubsubIOWithoutAttributes(
  name: String,
  idAttribute: String,
  timestampAttribute: String
) extends PubsubIOWithoutAttributes[String] {
  override protected def read(sc: ScioContext, params: ReadP): SCollection[String] = {
    val t = setup(beam.PubsubIO.readStrings(), params)
    sc.wrap(sc.applyInternal(t))
  }

  override protected def write(data: SCollection[String], params: WriteP): Tap[Nothing] = {
    val t = setup(beam.PubsubIO.writeStrings(), params)
    data
      .asInstanceOf[SCollection[String]]
      .applyInternal(t)
    EmptyTap
  }
}

private final case class AvroPubsubIOWithoutAttributes[T <: SpecificRecordBase: ClassTag](
  name: String,
  idAttribute: String,
  timestampAttribute: String
) extends PubsubIOWithoutAttributes[T] {
  private[this] val cls = ScioUtil.classOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val t = setup(beam.PubsubIO.readAvros(cls), params)
    sc.wrap(sc.applyInternal(t))
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    val t = setup(beam.PubsubIO.writeAvros(cls), params)
    data.applyInternal(t)
    EmptyTap
  }
}

private final case class MessagePubsubIOWithoutAttributes[T <: Message: ClassTag](
  name: String,
  idAttribute: String,
  timestampAttribute: String
) extends PubsubIOWithoutAttributes[T] {
  private[this] val cls = ScioUtil.classOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val t = setup(beam.PubsubIO.readProtos(cls.asSubclass(classOf[Message])), params)
    sc.wrap(sc.applyInternal(t)).asInstanceOf[SCollection[T]]
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    val t = setup(beam.PubsubIO.writeProtos(cls.asInstanceOf[Class[Message]]), params)
    data.asInstanceOf[SCollection[Message]].applyInternal(t)
    EmptyTap
  }
}

private final case class PubSubMessagePubsubIOWithoutAttributes[T <: beam.PubsubMessage: ClassTag](
  name: String,
  idAttribute: String,
  timestampAttribute: String
) extends PubsubIOWithoutAttributes[T] {
  private[this] val cls = ScioUtil.classOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val t = setup(beam.PubsubIO.readMessages(), params)
    sc.wrap(sc.applyInternal(t)).asInstanceOf[SCollection[T]]
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    val t = setup(beam.PubsubIO.writeMessages(), params)
    data.asInstanceOf[SCollection[beam.PubsubMessage]].applyInternal(t)
    EmptyTap
  }
}

private final case class FallbackPubsubIOWithoutAttributes[T: ClassTag: Coder](
  name: String,
  idAttribute: String,
  timestampAttribute: String
) extends PubsubIOWithoutAttributes[T] {
  private[this] val cls = ScioUtil.classOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, Coder[T])
    val t = setup(
      beam.PubsubIO.readMessagesWithCoderAndParseFn(
        coder,
        Functions.simpleFn(m => CoderUtils.decodeFromByteArray(coder, m.getPayload))
      ),
      params
    )

    sc.wrap(sc.applyInternal(t))
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    val coder = CoderMaterializer.beam(data.context, Coder[T])
    val t = setup(beam.PubsubIO.writeMessages(), params)
    data
      .map { record =>
        val payload = CoderUtils.encodeToByteArray(coder, record)
        new beam.PubsubMessage(payload, Map.empty[String, String].asJava)
      }
      .applyInternal(t)
    EmptyTap
  }
}

private final case class PubsubIOWithAttributes[T: ClassTag: Coder](
  name: String,
  idAttribute: String,
  timestampAttribute: String
) extends PubsubIO[(T, Map[String, String])] {
  type WithAttributeMap = (T, Map[String, String])

  override def testId: String =
    s"PubsubIO($name, $idAttribute, $timestampAttribute)"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[WithAttributeMap] = {
    var r = beam.PubsubIO.readMessagesWithAttributes()
    r = params.readType match {
      case Subscription => r.fromSubscription(name)
      case Topic        => r.fromTopic(name)
    }
    r = PubsubIO.setAttrs(r)(idAttribute, timestampAttribute)

    val coder = CoderMaterializer.beam(sc, Coder[T])
    sc.wrap(sc.applyInternal(r))
      .map { m =>
        val payload = CoderUtils.decodeFromByteArray(coder, m.getPayload)
        val attributes = JMapWrapper.of(m.getAttributeMap)
        (payload, attributes)
      }
  }

  override def readTest(sc: ScioContext, params: ReadP)(
    implicit coder: Coder[WithAttributeMap]
  ): SCollection[WithAttributeMap] = {
    val read = TestDataManager.getInput(sc.testId.get)(this).toSCollection(sc)

    Option(timestampAttribute)
      .map { att =>
        read.timestampBy(kv => new Instant(kv._2(att)))
      }
      .getOrElse(read)
  }

  override protected def write(
    data: SCollection[WithAttributeMap],
    params: WriteP
  ): Tap[Nothing] = {
    val w =
      PubsubIO.setAttrs(beam.PubsubIO.writeMessages().to(name))(idAttribute, timestampAttribute)

    val coder = CoderMaterializer.beam(data.context, Coder[T])

    data.applyInternal(new PTransform[PCollection[WithAttributeMap], PDone]() {
      override def expand(input: PCollection[WithAttributeMap]): PDone =
        input
          .apply(
            "Encode Pubsub message and attributes",
            ParDo.of(Functions.mapFn[WithAttributeMap, beam.PubsubMessage] { kv =>
              val payload = CoderUtils.encodeToByteArray(coder, kv._1)
              val attributes = kv._2.asJava
              new beam.PubsubMessage(payload, attributes)
            })
          )
          .setCoder(PubsubMessageWithAttributesCoder.of())
          .apply("Write to Pubsub", w)
    })

    EmptyTap
  }
}
