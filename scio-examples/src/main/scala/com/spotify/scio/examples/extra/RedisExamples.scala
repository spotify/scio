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
 *
 */

package com.spotify.scio.examples.extra

import com.spotify.scio.{ContextAndArgs, ScioContext}
import com.spotify.scio.pubsub._
import com.spotify.scio.redis._
import com.spotify.scio.redis.types._
import com.spotify.scio.redis.coders._
import org.apache.beam.examples.common.ExampleUtils
import org.apache.beam.sdk.options.{PipelineOptions, StreamingOptions}
import scala.concurrent.{ExecutionContext, Future}

// ## Redis Read Strings example
// Read strings from Redis by a key pattern

// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.RedisReadStringsExample
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --redisHost=[REDIS_HOST]
// --redisPort=[REDIS_PORT]
// --keyPattern=[KEY_PATTERN]
// --output=gs://[BUCKET]/[PATH]/redis_strings"`
object RedisReadStringsExample {

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val redisHost = args("redisHost")
    val redisPort = args.int("redisPort")
    val keyPattern = args("keyPattern")
    val connectionOptions = RedisConnectionOptions(redisHost, redisPort)

    sc.redis(connectionOptions, keyPattern)
      .debug()
      .saveAsTextFile(args("output"))

    sc.run()
    ()
  }

}

// ## Redis Write Strings example
// Write strings to Redis

// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.RedisWriteBatchExample
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --redisHost=[REDIS_HOST]
// --redisPort=[REDIS_PORT]`
object RedisWriteBatchExample {

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val redisHost = args("redisHost")
    val redisPort = args.int("redisPort")
    val connectionOptions = RedisConnectionOptions(redisHost, redisPort)

    sc.parallelize(
      Iterable(
        Append("key1", "1"),
        Append("key2".getBytes(), "2".getBytes())
      )
    ).saveAsRedis(connectionOptions)

    sc.run()
    ()
  }

}

// ## Streaming Redis Write Strings example
// Keeps a running counter of distinct strings coming from a PubSub topic.

// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.RedisWriteStreamingExample
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --subscription=[PUBSUB_SUBSCRIPTION]
// --redisHost=[REDIS_HOST]
// --redisPort=[REDIS_PORT]`
object RedisWriteStreamingExample {

  def main(cmdlineArgs: Array[String]): Unit = {

    val (opts, args) = ScioContext.parseArguments[PipelineOptions](cmdlineArgs)
    opts.as(classOf[StreamingOptions]).setStreaming(true)
    val exampleUtils = new ExampleUtils(opts)

    val sc = ScioContext(opts)

    val redisHost = args("redisHost")
    val redisPort = args.int("redisPort")
    val pubSubSubscription = args("subscription")

    val connectionOptions = RedisConnectionOptions(redisHost, redisPort)

    val params = PubsubIO.ReadParam(isSubscription = true)
    sc.read(PubsubIO.string(pubSubSubscription))(params)
      .flatMap(_.split(" "))
      .filter(_.length > 0)
      .map(IncrBy(_, 1))
      .debug()
      .saveAsRedis(connectionOptions)

    val result = sc.run()
    exampleUtils.waitToFinish(result.pipelineResult)
  }
}

// ## Redis Lookup Strings example

// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.RedisLookUpStringsExample
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --redisHost=[REDIS_HOST]
// --redisPort=[REDIS_PORT]
object RedisLookUpStringsExample {

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val redisHost = args("redisHost")
    val redisPort = args.int("redisPort")
    val connectionOptions = RedisConnectionOptions(redisHost, redisPort)

    sc.parallelize(Seq("key1", "key2", "unknownKey"))
      // #RedisLookup_example
      .parDo(
        new RedisDoFn[String, (String, Option[String])](connectionOptions, 1000) {
          override def request(value: String, client: Client)(implicit
            ec: ExecutionContext
          ): Future[(String, Option[String])] =
            client
              .request(p => p.get(value) :: Nil)
              .map { case r: List[String @unchecked] => (value, r.headOption) }
        }
      )
      // #RedisLookup_example
      .debug()

    sc.run()
    ()
  }

}
