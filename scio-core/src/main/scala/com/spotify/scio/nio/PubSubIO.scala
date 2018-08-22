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

  type ReadP = ReadParams
  type WriteP = Unit

  def tap(params: ReadParams): Tap[T] = ???
}

object PubSubIO {
  def apply[T: ClassTag](name: String,
                         idAttribute: String = null,
                         timestampAttribute: String = null): PubSubIO[T] =
    new PubSubIOWithoutAttributes[T](name, idAttribute, timestampAttribute)

  def withAttributes[T: ClassTag](name: String,
                                  idAttribute: String = null,
                                  timestampAttribute: String = null)
  : PubSubIO[(T, Map[String, String])] =
    new PubSubIOWithAttributes[T](name, idAttribute, timestampAttribute)
}

private case class PubSubIOWithoutAttributes[T: ClassTag](name: String,
                                                          idAttribute: String,
                                                          timestampAttribute: String)
  extends PubSubIO[T] {
  private val cls = ScioUtil.classOf[T]

  def id: String = name

  def read(sc: ScioContext, params: ReadP): SCollection[T] = sc.requireNotClosed {
    if (sc.isTest) {
      sc.getTestInput(this.asInstanceOf[ScioIO[T]])
    } else {
      def setup[U](read: BeamPubSubIO.Read[U]) = {
        var r = read
        r = if (params.isSubscription) r.fromSubscription(name) else r.fromTopic(name)
        if (idAttribute != null) {
          r = r.withIdAttribute(idAttribute)
        }
        if (timestampAttribute != null) {
          r = r.withTimestampAttribute(timestampAttribute)
        }
        r
      }

      if (classOf[String] isAssignableFrom cls) {
        val t = setup(BeamPubSubIO.readStrings())
        sc.wrap(sc.applyInternal(t)).setName(name).asInstanceOf[SCollection[T]]
      } else if (classOf[SpecificRecordBase] isAssignableFrom cls) {
        val t = setup(BeamPubSubIO.readAvros(cls))
        sc.wrap(sc.applyInternal(t)).setName(name)
      } else if (classOf[Message] isAssignableFrom cls) {
        val t = setup(BeamPubSubIO.readProtos(cls.asSubclass(classOf[Message])))
        sc.wrap(sc.applyInternal(t)).setName(name).asInstanceOf[SCollection[T]]
      } else {
        val coder = sc.pipeline.getCoderRegistry.getScalaCoder[T](sc.options)
        val t = setup(BeamPubSubIO.readMessages())
        sc.wrap(sc.applyInternal(t)).setName(name)
          .map(m => CoderUtils.decodeFromByteArray(coder, m.getPayload))
      }
    }
  }

  def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = {
    if (data.context.isTest) {
      data.context.testOut[T](this.asInstanceOf[ScioIO[T]])
    } else {
      def setup[U](write: BeamPubSubIO.Write[U]) = {
        var w = write.to(name)
        if (idAttribute != null) {
          w = w.withIdAttribute(idAttribute)
        }
        if (timestampAttribute != null) {
          w = w.withTimestampAttribute(timestampAttribute)
        }
        w
      }
      if (classOf[String] isAssignableFrom cls) {
        val t = setup(BeamPubSubIO.writeStrings())
        data
          .asInstanceOf[SCollection[String]]
          .applyInternal(t)
      } else if (classOf[SpecificRecordBase] isAssignableFrom cls) {
        val t = setup(BeamPubSubIO.writeAvros(cls))
        data.applyInternal(t)
      } else if (classOf[Message] isAssignableFrom cls) {
        val t = BeamPubSubIO.writeProtos(cls.asInstanceOf[Class[Message]])
        data.asInstanceOf[SCollection[Message]].applyInternal(t)
      } else {
        val coder = data.internal.getPipeline.getCoderRegistry
          .getScalaCoder[T](data.context.options)
        val t = setup(BeamPubSubIO.writeMessages())
        data.map { record =>
          val payload = CoderUtils.encodeToByteArray(coder, record)
          new PubsubMessage(payload, Map.empty[String, String].asJava)
        }.applyInternal(t)
      }
    }
    Future.failed(new NotImplementedError("Pubsub future not implemented"))
  }
}

private case class PubSubIOWithAttributes[T: ClassTag](name: String,
                                                       idAttribute: String,
                                                       timestampAttribute: String)
  extends PubSubIO[(T, Map[String, String])] {
  type WithAttributeMap = (T, Map[String, String])

  def id: String = name

  def read(sc: ScioContext, params: ReadP): SCollection[WithAttributeMap] =
    sc.requireNotClosed {
      if (sc.isTest) {
        sc.getTestInput(this.asInstanceOf[ScioIO[WithAttributeMap]])
      } else {
        var r = BeamPubSubIO.readMessagesWithAttributes()
        r = if (params.isSubscription) r.fromSubscription(name) else r.fromTopic(name)
        if (idAttribute != null) {
          r = r.withIdAttribute(idAttribute)
        }
        if (timestampAttribute != null) {
          r = r.withTimestampAttribute(timestampAttribute)
        }

        val elementCoder = sc.pipeline.getCoderRegistry.getScalaCoder[T](sc.options)
        sc.wrap(sc.applyInternal(r)).setName(name)
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
      var w = BeamPubSubIO.writeMessages().to(name)
      if (idAttribute != null) {
        w = w.withIdAttribute(idAttribute)
      }
      if (timestampAttribute != null) {
        w = w.withTimestampAttribute(timestampAttribute)
      }
      val coder = data.internal.getPipeline.getCoderRegistry.getScalaCoder[T](data.context.options)
      data.map { kv =>
        val payload = CoderUtils.encodeToByteArray(coder, kv._1)
        val attributes = kv._2.asJava
        new PubsubMessage(payload, attributes)
      }.applyInternal(w)
    }
    Future.failed(new NotImplementedError("Pubsub future not implemented"))
  }
}
