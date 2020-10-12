package com.spotify.scio.examples.extra


import com.spotify.scio.memcached.MemcacheConnectionOptions
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.memcached._
import com.spotify.scio.{ContextAndArgs, ScioContext}


// `sbt "runMain com.spotify.scio.examples.extra.MemcacheWriteExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --memcahceHost=[MEMCACHE_HOST]
// --memcachPort=[MEMCACHE_PORT]`

//10.72.48.3:11211
object MemcacheWriteExample {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val memcacheHost = args("memcahceHost")
    val memcachePort = args.int("memcahcePort")
    val ttl = 20
    val flushDelay = 40
    val memcacheConnectionOptions = MemcacheConnectionOptions(memcacheHost, memcachePort, ttl, flushDelay)

    sc.parallelize(
      Iterable(
        "key1" -> "1",
        "key2" -> "2",
        "key3" -> "3"
      )
    ).saveAsMemcache(memcacheConnectionOptions)
    val tmp = sc.run()
    tmp.waitUntilDone()

  }

}

//sbt project scio-examples runMain com.spotify.scio.examples.extra.MemcacheWriteExample --project=acmacquisition --runner=DataflowRunner --zone=europe-north1 --memcahceHost=10.72.48.3 --memcachPort=11211"