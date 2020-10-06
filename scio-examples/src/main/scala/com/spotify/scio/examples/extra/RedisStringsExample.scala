package com.spotify.scio.examples.extra

import com.spotify.scio.{ContextAndArgs, ScioContext}
import com.spotify.scio.redis._
import org.apache.beam.examples.common.ExampleUtils
import org.apache.beam.sdk.io.redis.RedisIO
import org.apache.beam.sdk.options.{PipelineOptions, StreamingOptions}
import com.spotify.scio.pubsub._

// ## Redis Read Strings example
// Read strings from Redis by a key pattern

// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.RedisReadStringsExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
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

    sc.redis(redisHost, redisPort, keyPattern)
      .debug()
      .saveAsTextFile(args("output"))

    sc.run()
    ()
  }

}

// ## Redis Write Strings example
// Write strings to Redis

// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.RedisWriteStringsExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --redisHost=[REDIS_HOST]
// --redisPort=[REDIS_PORT]`
object RedisWriteStringsExample {

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val redisHost = args("redisHost")
    val redisPort = args.int("redisPort")

    sc.parallelize(Iterable(
      "key1" -> "1", "key2" -> "2", "key3" -> "3"
    )).saveAsRedis(redisHost, redisPort, RedisIO.Write.Method.APPEND)

    sc.run()
    ()
  }

}

// ## Streaming Redis Write Strings example
// Keeps a running counter of distinct strings coming from a PubSub topic.

// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.RedisWriteStringsStreamingExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --subscription=[PUBSUB_SUBSCRIPTION]
// --redisHost=[REDIS_HOST]
// --redisPort=[REDIS_PORT]`
object RedisWriteStringsStreamingExample {

  def main(cmdlineArgs: Array[String]): Unit = {

    val (opts, args) = ScioContext.parseArguments[PipelineOptions](cmdlineArgs)
    opts.as(classOf[StreamingOptions]).setStreaming(true)
    val exampleUtils = new ExampleUtils(opts)

    val sc = ScioContext(opts)

    val redisHost = args("redisHost")
    val redisPort = args.int("redisPort")
    val pubSubSubscription = args("subscription")

    val params = PubsubIO.ReadParam(isSubscription = true)
    sc.read(PubsubIO.string(pubSubSubscription))(params)
      .debug()
      .map(msg => msg -> "1")
      .saveAsRedis(redisHost, redisPort, RedisIO.Write.Method.INCRBY)

    val result = sc.run()
    exampleUtils.waitToFinish(result.pipelineResult)
  }
}
