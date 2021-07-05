package com.spotify.scio.examples.extra


import com.spotify.scio.memcached._
import com.spotify.scio._


// `sbt "runMain com.spotify.scio.examples.extra.MemcacheWriteExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --memcacheHost=[MEMCACHE_HOST]

object MemcacheWriteExample {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val mHost = args("memcacheHost")
    val ttl = 20
    val flushDelay = 40
    val memcacheConnectionOptions = MemcacheConnectionOptions.withDefaultPort(mHost, ttl, flushDelay)

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
