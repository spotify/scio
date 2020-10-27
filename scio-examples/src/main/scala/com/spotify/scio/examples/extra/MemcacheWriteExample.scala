package com.spotify.scio.examples.extra


import com.spotify.scio.memcached._
import com.spotify.scio._


// `sbt "runMain com.spotify.scio.examples.extra.MemcacheWriteExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --memcacheHost=[MEMCACHE_HOST]
// --memcachePort=[MEMCACHE_PORT]`

object MemcacheWriteExample {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val mHost = args("memcacheHost")
    val mPort = args.int("memcachePort")
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
    sc.run()
    ()

  }

}
