package com.spotify.scio.examples.extra


import com.spotify.scio.memcached._
import com.spotify.scio._


// `sbt "runMain com.spotify.scio.examples.extra.MemcacheWriteExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --memcahceHost=[MEMCACHE_HOST]
// --memcachPort=[MEMCACHE_PORT]`

object MemcacheWriteExample {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val mHost = args("memcahceHost")
    val mPort = args.int("memcahcePort")
    val ttl = 20
    val flushDelay = 40
    val memcacheConnectionOptions = MemcacheConnectionOptions(mHost, mPort, ttl, flushDelay)

    sc.parallelize(
      Iterable(
        "key1" -> "1",
        "key2" -> "2",
        "key3" -> "3"
      )
    ).saveAsMemcache(memcacheConnectionOptions)
    val tmp = sc.run().waitUntilDone()

  }

}
