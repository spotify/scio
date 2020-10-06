package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.redis._
import org.apache.beam.sdk.io.redis.RedisIO

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
      //.saveAsTextFile(args("output"))


    sc.run()
    ()
  }

}

// ## Redis Write Strings example
// Read strings from Redis by a key pattern

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
