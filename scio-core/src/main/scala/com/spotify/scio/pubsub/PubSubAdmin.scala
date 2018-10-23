/*
 * Copyright 2017 Spotify AB.
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

import com.google.pubsub.v1.PublisherGrpc.PublisherBlockingStub
import com.google.pubsub.v1.SubscriberGrpc.SubscriberBlockingStub
import com.google.pubsub.v1.{
  GetSubscriptionRequest,
  GetTopicRequest,
  PublisherGrpc,
  SubscriberGrpc,
  Subscription,
  Topic
}
import io.grpc.ManagedChannel
import io.grpc.auth.MoreCallCredentials
import io.grpc.netty.{GrpcSslContexts, NegotiationType, NettyChannelBuilder}
import org.apache.beam.sdk.io.gcp.pubsub.PubsubOptions

import scala.util.Try

object PubSubAdmin {

  private object GrpcClient {
    private def newChannel: ManagedChannel = {
      NettyChannelBuilder
        .forAddress("pubsub.googleapis.com", 443)
        .negotiationType(NegotiationType.TLS)
        .sslContext(GrpcSslContexts.forClient.ciphers(null).build)
        .build
    }

    def subscriber[A](pubsubOptions: PubsubOptions)(f: SubscriberBlockingStub => A): Try[A] = {
      val channel = newChannel
      val client = SubscriberGrpc
        .newBlockingStub(channel)
        .withCallCredentials(MoreCallCredentials.from(pubsubOptions.getGcpCredential))

      val result = Try(f(client))
      channel.shutdownNow()
      result
    }

    def publisher[A](pubsubOptions: PubsubOptions)(f: PublisherBlockingStub => A): Try[A] = {
      val channel = newChannel
      val client = PublisherGrpc
        .newBlockingStub(channel)
        .withCallCredentials(MoreCallCredentials.from(pubsubOptions.getGcpCredential))

      val result = Try(f(client))
      channel.shutdownNow()
      result
    }
  }

  /**
   * Ensure PubSub topic exists.
   *
   * @param name Topic path. Needs to follow `projects/projectId/topics/topicName`
   * @return a Topic if it alreadys exists or has been successfully created, a failure otherwise
   */
  def ensureTopic(pubsubOptions: PubsubOptions, name: String): Try[Topic] =
    GrpcClient.publisher(pubsubOptions) { client =>
      val topic = Topic.newBuilder().setName(name).build()
      client.createTopic(topic)
    }

  /**
   * Get the configuration of a topic.
   *
   * @param name Topic path. Needs to follow `projects/projectId/topics/topicName`
   * @return a Topic if it exists, a failure otherwise
   */
  def topic(pubsubOptions: PubsubOptions, name: String): Try[Topic] =
    GrpcClient.publisher(pubsubOptions) { client =>
      val topicRequest = GetTopicRequest.newBuilder().setTopic(name).build()
      client.getTopic(topicRequest)
    }

  /**
   * Ensure PubSub subscription exists.
   *
   * @param topic Topic path. Needs to follow `projects/projectId/topics/topicName`
   * @param name  Subscription path. Needs to follow `projects/projectId/topics/topicName`
   * @return a Subscription if it alreadys exists or has been successfully created, a failure
   * otherwise
   */
  def ensureSubscription(pubsubOptions: PubsubOptions,
                         topic: String,
                         name: String): Try[Subscription] =
    GrpcClient.subscriber(pubsubOptions) { client =>
      val sub = Subscription.newBuilder().setTopic(topic).setName(name).build()
      client.createSubscription(sub)
    }

  /**
   * Get the configuration of a subscription.
   *
   * @param name Subscription path. Needs to follow `projects/projectId/subscriptions/subscription`
   * @return a Subscription if it exists, a failure otherwise
   */
  def subscription(pubsubOptions: PubsubOptions, name: String): Try[Subscription] =
    GrpcClient.subscriber(pubsubOptions) { client =>
      val subRequest = GetSubscriptionRequest.newBuilder().setSubscription(name).build()
      client.getSubscription(subRequest)
    }
}
