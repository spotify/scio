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

package com.spotify.scio.pubsub.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.Coder
import com.spotify.scio.pubsub.PubsubIO

import scala.reflect.ClassTag

final class ScioContextOps(private val sc: ScioContext) extends AnyVal {
  private def pubsubIn[T: ClassTag: Coder](
    isSubscription: Boolean,
    name: String,
    idAttribute: String,
    timestampAttribute: String
  ): SCollection[T] = {
    val io = PubsubIO[T](name, idAttribute, timestampAttribute)
    sc.read(io)(PubsubIO.ReadParam(isSubscription))
  }

  /**
   * Get an SCollection for a Pub/Sub subscription.
   * @group input
   */
  def pubsubSubscription[T: ClassTag: Coder](
    sub: String,
    idAttribute: String = null,
    timestampAttribute: String = null
  ): SCollection[T] =
    pubsubIn(isSubscription = true, sub, idAttribute, timestampAttribute)

  /**
   * Get an SCollection for a Pub/Sub topic.
   * @group input
   */
  def pubsubTopic[T: ClassTag: Coder](
    topic: String,
    idAttribute: String = null,
    timestampAttribute: String = null
  ): SCollection[T] =
    pubsubIn(isSubscription = false, topic, idAttribute, timestampAttribute)

  private def pubsubInWithAttributes[T: ClassTag: Coder](
    isSubscription: Boolean,
    name: String,
    idAttribute: String,
    timestampAttribute: String
  ): SCollection[(T, Map[String, String])] = {
    val io = PubsubIO.withAttributes[T](name, idAttribute, timestampAttribute)
    sc.read(io)(PubsubIO.ReadParam(isSubscription))
  }

  /**
   * Get an SCollection for a Pub/Sub subscription that includes message attributes.
   * @group input
   */
  def pubsubSubscriptionWithAttributes[T: ClassTag: Coder](
    sub: String,
    idAttribute: String = null,
    timestampAttribute: String = null
  ): SCollection[(T, Map[String, String])] =
    pubsubInWithAttributes[T](isSubscription = true, sub, idAttribute, timestampAttribute)

  /**
   * Get an SCollection for a Pub/Sub topic that includes message attributes.
   * @group input
   */
  def pubsubTopicWithAttributes[T: ClassTag: Coder](
    topic: String,
    idAttribute: String = null,
    timestampAttribute: String = null
  ): SCollection[(T, Map[String, String])] =
    pubsubInWithAttributes[T](isSubscription = false, topic, idAttribute, timestampAttribute)

}

trait ScioContextSyntax {
  implicit def pubSubScioContextOps(sc: ScioContext): ScioContextOps = new ScioContextOps(sc)
}
