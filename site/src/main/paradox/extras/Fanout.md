# Fanout

Scio ships with two `SCollection` variants that provide _fanout_ over aggregations where an interim aggregation is performed before the final aggregation is computed.
The interim step pairs the data to be aggregated with a synthetic key, then aggregates within this artificial keyspace before passing the partial aggregations on to the final aggregation step.
The interim step requires an additional shuffle but can make the aggregation more parallelizable and reduces the impact of a hot key.

The `aggregate`, `combine`, `fold`, `reduce`, `sum` transforms and their keyed variants are supported.

## WithFanout

@scaladoc[withFanout](com.spotify.scio.values.SCollection#withFanout(fanout:Int):com.spotify.scio.values.SCollectionWithFanout[T]) aggregates over the number of synthetic keys specified by the `fanout` argument:

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection

val elements: SCollection[Int] = ???
val result: SCollection[Int] = elements.withFanout(fanout = 10).sum
```

## WithHotKeyFanout

For hot keys, two variants allow a user to specify either a static fanout via an integer `hotKeyFanout` argument to @scaladoc[withHotKeyFanout](com.spotify.scio.values.PairSCollectionFunctions#withHotKeyFanout(hotKeyFanout:Int):com.spotify.scio.values.SCollectionWithHotKeyFanout[K,V]), or a dynamic per-key fanout via a function `K => Int` argument, also called `hotKeyFanout` to @scaladoc[withHotKeyFanout](com.spotify.scio.values.PairSCollectionFunctions#withHotKeyFanout(hotKeyFanout:K=%3EInt):com.spotify.scio.values.SCollectionWithHotKeyFanout[K,V]):

```scala
import com.spotify.scio._
import com.spotify.scio.values.SCollection

val elements: SCollection[(String, Int)] = ???
val staticResult: SCollection[(String, Int)] = elements.withHotKeyFanout(hotKeyFanout = 10).sumByKey
val dynamicResult: SCollection[(String, Int)] = elements
  .withHotKeyFanout(hotKeyFanout = s => s.length % 10)
  .sumByKey
```
