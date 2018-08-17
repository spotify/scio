/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.nio

import com.google.protobuf.Message
import com.spotify.scio.Implicits._
import com.spotify.scio.ScioContext
import com.spotify.scio.io.Tap
import com.spotify.scio.util.{JMapWrapper, ScioUtil}
import com.spotify.scio.values.SCollection
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.io.gcp.pubsub.{PubsubMessage, PubsubIO => BeamPubSubIO}
import org.apache.beam.sdk.util.CoderUtils

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.reflect.ClassTag

sealed trait PubSubIO[T] extends ScioIO[T] {
  case class ReadParams(isSubscription: Boolean)
  case class WriteParams()

  type ReadP = ReadParams
  type WriteP = WriteParams

  def tap(params: ReadParams): Tap[T] = new Tap[T] {
    override def value: Iterator[T] =
      throw new NotImplementedError("Pubsub tap cannot be loaded into memory")

    override def open(sc: ScioContext): SCollection[T] = read(sc, params)
  }
}

object PubSubIO {
  def apply[T: ClassTag](topic: String): PubSubIO[T] = new PubSubIOWithoutAttributes[T](topic)

  def withAttributes[T: ClassTag](
                                   topic: String,
                                   idAttribute: String = null,
                                   timestampAttribute: String = null
                                 ): PubSubIO[(T, Map[String, String])] =
    new PubSubIOWithAttributes[T](topic, idAttribute, timestampAttribute)
}

case class PubSubIOWithoutAttributes[T: ClassTag](topic: String) extends PubSubIO[T] {
  def id: String = topic

  def read(sc: ScioContext, params: ReadP): SCollection[T] = sc.requireNotClosed {
    if (sc.isTest) {
      sc.getTestInput(this.asInstanceOf[ScioIO[T]])
    } else {
      val cls = getTpe[T]

      def setup[U](reader: BeamPubSubIO.Read[U]) = {
        if (params.isSubscription) {
          reader.fromSubscription(topic)
        } else {
          reader.fromTopic(topic)
        }
      }

      if (classOf[String] isAssignableFrom cls) {
        val t = setup(BeamPubSubIO.readStrings())
        sc.wrap(sc.applyInternal(t)).setName(topic).asInstanceOf[SCollection[T]]
      } else if (classOf[SpecificRecordBase] isAssignableFrom cls) {
        val t = setup(BeamPubSubIO.readAvros(cls))
        sc.wrap(sc.applyInternal(t)).setName(topic).asInstanceOf[SCollection[T]]
      } else if (classOf[Message] isAssignableFrom cls) {
        val t = setup(BeamPubSubIO.readProtos(cls.asSubclass(classOf[Message])))
        sc.wrap(sc.applyInternal(t)).setName(topic).asInstanceOf[SCollection[T]]
      } else {
        val coder = sc.pipeline.getCoderRegistry.getScalaCoder[T](sc.options)
        val t = setup(BeamPubSubIO.readMessages())
        sc.wrap(sc.applyInternal(t)).setName(topic)
          .map(m => CoderUtils.decodeFromByteArray(coder, m.getPayload))
      }
    }
  }

  def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = {
    if (data.context.isTest) {
      data.context.testOut[T](this.asInstanceOf[ScioIO[T]])
    } else {
      val cls = getTpe[T]

      if (classOf[String] isAssignableFrom cls) {
        data
          .asInstanceOf[SCollection[String]]
          .applyInternal(BeamPubSubIO.writeStrings().to(topic))
      } else if (classOf[SpecificRecordBase] isAssignableFrom cls) {
        data
          .applyInternal(BeamPubSubIO.writeAvros(cls).to(topic))
      } else if (classOf[Message] isAssignableFrom cls) {
        data
          .asInstanceOf[SCollection[Message]]
          .applyInternal(BeamPubSubIO.writeProtos(cls.asInstanceOf[Class[Message]]).to(topic))
      } else {
        val coder = data.internal.getPipeline.getCoderRegistry
          .getScalaCoder[T](data.context.options)

        data.map { record =>
          val payload = CoderUtils.encodeToByteArray(coder, record)
          new PubsubMessage(payload, Map.empty[String, String].asJava)
        }.applyInternal(BeamPubSubIO.writeMessages().to(topic))
      }
    }
    Future.failed(new NotImplementedError("Pubsub future not implemented"))
  }

  private def getTpe[T: ClassTag]: Class[T] = ScioUtil.classOf[T]
}

case class PubSubIOWithAttributes[T: ClassTag](
                                    topic: String,
                                    idAttribute: String = null,
                                    timestampAttribute: String = null
                                    ) extends PubSubIO[(T, Map[String, String])] {
  type WithAttributeMap = (T, Map[String, String])

  def id: String = topic

  def read(sc: ScioContext, params: ReadP): SCollection[WithAttributeMap] =
    sc.requireNotClosed {
      if (sc.isTest) {
        sc.getTestInput(this.asInstanceOf[ScioIO[WithAttributeMap]])
      } else {
        val reader = BeamPubSubIO.readMessagesWithAttributes()
        val transform = if (params.isSubscription) {
          reader.fromSubscription(topic)
        } else {
          reader.fromTopic(topic)
        }
        Option(idAttribute).foreach(transform.withIdAttribute)
        Option(timestampAttribute).foreach(transform.withTimestampAttribute)

        val elementCoder = sc.pipeline.getCoderRegistry.getScalaCoder[T](sc.options)
        sc.wrap(sc.applyInternal(transform)).setName(topic)
          .map { m =>
            val payload = CoderUtils.decodeFromByteArray(elementCoder, m.getPayload)
            val attributes = JMapWrapper.of(m.getAttributeMap)
            (payload, attributes)
          }
      }
    }

  def write(data: SCollection[WithAttributeMap], params: WriteP): Future[Tap[WithAttributeMap]] = {
    if (data.context.isTest) {
      data.context.testOut[T](this.asInstanceOf[ScioIO[T]])
    } else {
      val writer = BeamPubSubIO.writeMessages().to(topic)
      Option(idAttribute).foreach(writer.withIdAttribute)
      Option(timestampAttribute).foreach(writer.withTimestampAttribute)

      val coder = data.internal.getPipeline.getCoderRegistry
        .getScalaCoder[T](data.context.options)

      data.map { kv =>
        val payload = CoderUtils.encodeToByteArray(coder, kv._1)
        val attributes = kv._2.asJava
        new PubsubMessage(payload, attributes)
      }.applyInternal(writer)
    }
    Future.failed(new NotImplementedError("Pubsub future not implemented"))
  }
}
