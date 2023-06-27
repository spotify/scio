# Redis

Scio provides support for [Redis](https://redis.io/) in the `scio-redis` artifact.

# Batch read

Reading key-value pairs from redis for a specific key pattern is supported via @scaladoc[redis](com.spotify.scio.redis.syntax.ScioContextOps#redis(connectionOptions:com.spotify.scio.redis.RedisConnectionOptions,keyPattern:String,batchSize:Int,outputParallelization:Boolean):com.spotify.scio.values.SCollection[(String,String)]):

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.redis._

val sc: ScioContext = ???
val connectionOptions = RedisConnectionOptions("redisHost", 6379)
val keyPattern = "foo*"

val elements: SCollection[(String, String)] = sc.redis(connectionOptions, keyPattern)
```

# Lookups

Looking up specific keys from redis can be done with @scaladoc[RedisDoFn](com.spotify.scio.redis.RedisDoFn):

@@snip [RedisExamples.scala](/scio-examples/src/main/scala/com/spotify/scio/examples/extra/RedisExamples.scala) { #RedisLookup_example }

# Write

Writes to Redis require an `SCollection` of a subclass of @scaladoc[RedisMutation](com.spotify.scio.redis.types.RedisMutation).
Writes work in both batch and streaming modes via @scaladoc[saveAsRedis](com.spotify.scio.redis.syntax.SCollectionRedisOps#saveAsRedis(connectionOptions:com.spotify.scio.redis.RedisConnectionOptions,batchSize:Int):com.spotify.scio.io.ClosedTap[Nothing]):

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.redis._
import com.spotify.scio.redis.types._

val connectionOptions = RedisConnectionOptions("redisHost", 6379)

val keys: SCollection[String] = ???
keys.map(IncrBy(_, 1)).saveAsRedis(connectionOptions)
```
