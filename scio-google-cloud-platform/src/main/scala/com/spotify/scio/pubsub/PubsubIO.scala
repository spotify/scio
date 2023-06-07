/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.pubsub

import com.google.protobuf.Message
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.util.{Functions, JMapWrapper, ScioUtil}
import com.spotify.scio.values.SCollection
import com.spotify.scio.io._
import com.spotify.scio.pubsub.coders._
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.io.gcp.{pubsub => beam}
import org.apache.beam.sdk.util.CoderUtils
import org.joda.time.Instant

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

sealed trait PubsubIO[T] extends ScioIO[T] {
  override type ReadP = PubsubIO.ReadParam
  override type WriteP = PubsubIO.WriteParam
  final override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}

object PubsubIO {
  sealed trait ReadType
  case object Subscription extends ReadType
  case object Topic extends ReadType

  final case class ReadParam private (
    readType: ReadType,
    clientFactory: Option[beam.PubsubClient.PubsubClientFactory] = ReadParam.DefaultClientFactory,
    deadLetterTopic: Option[String] = ReadParam.DefaultDeadLetterTopic
  ) {
    val isSubscription: Boolean = readType match {
      case Subscription => true
      case _            => false
    }
  }
  object ReadParam {
    val DefaultClientFactory: Option[beam.PubsubClient.PubsubClientFactory] = None
    val DefaultDeadLetterTopic: Option[String] = None

    // required for back compatibility
    def apply(isSubscription: Boolean): ReadParam =
      if (isSubscription) ReadParam(Subscription) else ReadParam(Topic)
  }

  final case class WriteParam private (
    maxBatchSize: Option[Int] = WriteParam.DefaultMaxBatchSize,
    maxBatchBytesSize: Option[Int] = WriteParam.DefaultMaxBatchBytesSize,
    clientFactory: Option[beam.PubsubClient.PubsubClientFactory] = WriteParam.DefaultClientFactory
  )

  object WriteParam {
    val DefaultMaxBatchSize: Option[Int] = None
    val DefaultMaxBatchBytesSize: Option[Int] = None
    val DefaultClientFactory: Option[beam.PubsubClient.PubsubClientFactory] = None
  }

  def string(
    name: String,
    idAttribute: String = null,
    timestampAttribute: String = null
  ): PubsubIO[String] =
    StringPubsubIOWithoutAttributes(name, idAttribute, timestampAttribute)

  def avro[T <: SpecificRecord: ClassTag](
    name: String,
    idAttribute: String = null,
    timestampAttribute: String = null
  ): PubsubIO[T] =
    AvroPubsubIOWithoutAttributes[T](name, idAttribute, timestampAttribute)

  def proto[T <: Message: ClassTag](
    name: String,
    idAttribute: String = null,
    timestampAttribute: String = null
  ): PubsubIO[T] =
    MessagePubsubIOWithoutAttributes[T](name, idAttribute, timestampAttribute)

  def pubsub[T <: beam.PubsubMessage: ClassTag](
    name: String,
    idAttribute: String = null,
    timestampAttribute: String = null
  ): PubsubIO[T] =
    PubSubMessagePubsubIOWithoutAttributes[T](name, idAttribute, timestampAttribute)

  def coder[T: Coder: ClassTag](
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

  private[pubsub] def configureRead[T](
    r: beam.PubsubIO.Read[T]
  )(
    name: String,
    params: ReadParam,
    idAttribute: String,
    timestampAttribute: String
  ): beam.PubsubIO.Read[T] = {
    var read =
      params.readType match {
        case Subscription => r.fromSubscription(name)
        case Topic        => r.fromTopic(name)
      }

    read = params.clientFactory.fold(read)(read.withClientFactory)
    read = params.deadLetterTopic.fold(read)(read.withDeadLetterTopic)
    read = Option(idAttribute).fold(read)(read.withIdAttribute)
    read = Option(timestampAttribute).fold(read)(read.withTimestampAttribute)

    read
  }

  private[pubsub] def configureWrite[T](
    w: beam.PubsubIO.Write[T]
  )(
    name: String,
    params: WriteParam,
    idAttribute: String,
    timestampAttribute: String
  ): beam.PubsubIO.Write[T] = {
    var write = w.to(name)

    write = params.maxBatchBytesSize.fold(write)(write.withMaxBatchBytesSize)
    write = params.maxBatchSize.fold(write)(write.withMaxBatchSize)
    write = params.clientFactory.fold(write)(write.withClientFactory)
    write = Option(idAttribute).fold(write)(write.withIdAttribute)
    write = Option(timestampAttribute).fold(write)(write.withTimestampAttribute)

    write
  }
}

sealed private trait PubsubIOWithoutAttributes[T] extends PubsubIO[T] {
  def name: String
  def idAttribute: String
  def timestampAttribute: String

  override def testId: String =
    s"PubsubIO($name, $idAttribute, $timestampAttribute)"

  protected def setup[U](
    read: beam.PubsubIO.Read[U],
    params: PubsubIO.ReadParam
  ): beam.PubsubIO.Read[U] =
    PubsubIO.configureRead(read)(name, params, idAttribute, timestampAttribute)

  protected def setup[U](
    write: beam.PubsubIO.Write[U],
    params: PubsubIO.WriteParam
  ): beam.PubsubIO.Write[U] =
    PubsubIO.configureWrite(write)(name, params, idAttribute, timestampAttribute)
}

final private case class StringPubsubIOWithoutAttributes(
  name: String,
  idAttribute: String,
  timestampAttribute: String
) extends PubsubIOWithoutAttributes[String] {
  override protected def read(sc: ScioContext, params: ReadP): SCollection[String] = {
    val coder = CoderMaterializer.beam(sc, Coder.stringCoder)
    val t = setup(beam.PubsubIO.readStrings(), params)
    sc.applyTransform(t).setCoder(coder)
  }

  override protected def write(data: SCollection[String], params: WriteP): Tap[Nothing] = {
    val t = setup(beam.PubsubIO.writeStrings(), params)
    data.applyInternal(t)
    EmptyTap
  }
}

final private case class AvroPubsubIOWithoutAttributes[T <: SpecificRecord: ClassTag](
  name: String,
  idAttribute: String,
  timestampAttribute: String
) extends PubsubIOWithoutAttributes[T] {
  private[this] val cls = ScioUtil.classOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, Coder.avroSpecificRecordCoder[T])
    val t = setup(beam.PubsubIO.readAvros(cls), params)
    sc.applyTransform(t).setCoder(coder)
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    val t = setup(beam.PubsubIO.writeAvros(cls), params)
    data.applyInternal(t)
    EmptyTap
  }
}

final private case class MessagePubsubIOWithoutAttributes[T <: Message: ClassTag](
  name: String,
  idAttribute: String,
  timestampAttribute: String
) extends PubsubIOWithoutAttributes[T] {
  private[this] val cls = ScioUtil.classOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, Coder.protoMessageCoder[T])
    val t = setup(beam.PubsubIO.readProtos(cls), params)
    sc.applyTransform(t).setCoder(coder)
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    val t = setup(beam.PubsubIO.writeProtos(cls), params)
    data.applyInternal(t)
    EmptyTap
  }
}

final private case class PubSubMessagePubsubIOWithoutAttributes[T <: beam.PubsubMessage](
  name: String,
  idAttribute: String,
  timestampAttribute: String
) extends PubsubIOWithoutAttributes[T] {
  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, coders.messageCoder)
    val t = setup(beam.PubsubIO.readMessages(), params)
    sc.applyTransform(t).setCoder(coder).contravary[T]
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    val t = setup(beam.PubsubIO.writeMessages(), params)
    data.covary[beam.PubsubMessage].applyInternal(t)
    EmptyTap
  }
}

final private case class FallbackPubsubIOWithoutAttributes[T: Coder](
  name: String,
  idAttribute: String,
  timestampAttribute: String
) extends PubsubIOWithoutAttributes[T] {
  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, Coder[T])
    val t = setup(
      beam.PubsubIO.readMessagesWithCoderAndParseFn(
        coder,
        Functions.simpleFn(m => CoderUtils.decodeFromByteArray(coder, m.getPayload))
      ),
      params
    )

    sc.applyTransform(t)
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

final private case class PubsubIOWithAttributes[T: ClassTag: Coder](
  name: String,
  idAttribute: String,
  timestampAttribute: String
) extends PubsubIO[(T, Map[String, String])] {
  type WithAttributeMap = (T, Map[String, String])

  override def testId: String =
    s"PubsubIO($name, $idAttribute, $timestampAttribute)"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[WithAttributeMap] = {
    val coder = CoderMaterializer.beam(sc, Coder[T])
    val r = PubsubIO.configureRead(beam.PubsubIO.readMessagesWithAttributes())(
      name,
      params,
      idAttribute,
      timestampAttribute
    )

    sc.applyTransform(r)
      .map { m =>
        val payload = CoderUtils.decodeFromByteArray(coder, m.getPayload)
        val attributes = JMapWrapper.of(m.getAttributeMap)
        (payload, attributes)
      }
  }

  override def readTest(sc: ScioContext, params: ReadP): SCollection[WithAttributeMap] = {
    val read = TestDataManager.getInput(sc.testId.get)(this).toSCollection(sc)

    Option(timestampAttribute)
      .map(att => read.timestampBy(kv => new Instant(kv._2(att))))
      .getOrElse(read)
  }

  override protected def write(
    data: SCollection[WithAttributeMap],
    params: WriteP
  ): Tap[Nothing] = {
    val w =
      PubsubIO.configureWrite(beam.PubsubIO.writeMessages())(
        name,
        params,
        idAttribute,
        timestampAttribute
      )

    val coder = CoderMaterializer.beam(data.context, Coder[T])
    data.transform_ { coll =>
      coll
        .withName("Encode Pubsub message and attributes")
        .map { kv =>
          val payload = CoderUtils.encodeToByteArray(coder, kv._1)
          val attributes = kv._2.asJava
          new beam.PubsubMessage(payload, attributes)
        }
        .applyInternal("Write to Pubsub", w)
    }
    EmptyTap
  }
}
