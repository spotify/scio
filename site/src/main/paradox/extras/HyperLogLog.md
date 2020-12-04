#HyperLogLog

**HyperLogLog** is an algorithm to estimate the cardinality of a large dataset. Counting distinct in a large dataset
requires linear space. HLL algorithm approximate the cardinality with a small memory footprint. 

[HyperLogLog++](https://research.google/pubs/pub40671/) is an improved version of `HyperLogLog` algorithms presented by Google. It more accurately estimates 
distinct count in very large and small data streams.  

HyperLogLgo++ algorithm has been integrated with Apache Beam with help of [ZetaSketch](https://github.com/google/zetasketch) library, which is comply with
[Google Cloud BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions) sketches. More about Apache beam integration 
can find [here](https://s.apache.org/hll-in-beam).

## HyperLogLog++ Integration.
Scio distinct count API has been extended to support HyperLogLog algorithms using ApproxDistinctCount Interface. `scio-extra` 
module provide two different implementations of this interface,

- com.spotify.scio.extra.hll.sketching.SketchHllPlusPlus
- com.spotify.scio.extra.hll.zetasketch.ZetaSketchHllPlusPlus 

`SketchHllPlusPlus` provide HyperLogLog++ implementation based on [Addthis' Stream-lib library](https://github.com/addthis/stream-lib) 
while `ZetaSketchHllPlusPlus` provide implementation basedon [ZetaSketch](https://github.com/google/zetasketch).

Following is how you can use the new API.

In this example, we count the distinct value of a given type using `SketchHllPlusPlus` implementation. 
```scala
import com.spotify.scio.extra.hll.sketching.SketchHllPlusPlus

val input: SCollection[T] = ...
val distinctCount: SCollection[Long] = 
        input
        .countApproxDistinct(new SketchHllPlusPlus(15, 20))
```

Same thing can be done using `ZetaSketchHllPlusPlus` too, but this only support `Int`, `Long`, `String` and `Array[Byte]` 
types only. Meaning you can only call this on `SCollection` of about supported types.
```scala
import com.spotify.scio.extra.hll.zetasketch.ZetaSketchHllPlusPlus

val estimator = ZetaSketchHllPlusPlus[Int]() 

val input: SCollection[Int] = ...
val distinctCount: SCollection[Long] = 
        input
        .countApproxDistinct(estimator)
```

Scio support the same for key-value `SCollection` too, in this case it will output distinct count per each unique key in 
the input data stream. 

```scala
import com.spotify.scio.extra.hll.sketching.SketchHllPlusPlus

val input: SCollection[(K, V)] = ...
val distinctCount: SCollection[(K, Long)] =
    input
    .countApproxDistinctByKey(new SketchHllPlusPlus[Int](15, 20))
```

Same thing with `ZetaSketchHllPlusPlus`


```scala
import com.spotify.scio.extra.hll.zetasketch.ZetaSketchHllPlusPlus

val estimator = ZetaSketchHllPlusPlus[Int]() 

val input: SCollection[(K, Int)] = ... // K could be any type, value should be one of ZetaSketch supported types.  
val distinctCount: SCollection[(K, Long)] =
    input
    .countApproxDistinctByKey(estimator)
```

## Distributed HyperLogLog++

Both above implementation estimate distinct count for the whole data stream and doesn't expose the underline sketch to the user.
Scio exposed the underline sketch to the user and make it possible to run this HLL++ algorithm in distributed way using the 
ZetaSketch library's internal APIs. Since, ZetaSketch is comply with [Gooogle Cloud BigQuery](https://cloud.google.com/bigquery/docs/reference/standard-sql/hll_functions), 
you can use BigQuery generated sketches with sketches exposed by this API.

```scala
import com.spotify.scio.extra.hll._

val input: SCollection[T] = ...
// first convert each element to ZetaSketchHLL.
val zCol: SCollection[ZetaSketchHll[T]] = input.asZetaSketchHll
// then combine all ZeatSketchHLL and count the distinct.
val approxDistCount: SCollection[Long] = zCol.sumHll.approxDistinctCount
```

or in simpler way you can do:

```scala
import com.spotify.scio.extra.hll._

val input: SCollection[T] = ...
val approxDistCount: SCollection[Long] = input.approxDistinctCountWithZetaHll
```

**Note**: This supports `Int`, `Long`, `String` and `ByteString` input types only.

Similarly, for key-value SCollections.
```scala
import com.spotify.scio.extra.hll._

val input: SCollection[(K, V)] = ... // Here type V should be one of supported type. `Int`, `Long`, `String` or `ByteString`
val zCol: SCollection[(K, ZetaSketchHLL[V])] = input.asZetaSketchHllByKey
val approxDistCount: SCollection[(K, Long)] = zCol.sumHllByKey.approxDistinctCountByKey

// or 
val approxDistCount: SCollection[(K, Long)] = input.approxDistinctCountWithZetaHllByKey
```

**NOTE**: `ZetaSketchLL[T]` has `algebird`'s `Monoid` and `Aggregator` implementations. Use it by importing 
`com.spotify.scio.extra.hll.zetasketch.ZetaSketchHLL._` to the scope.
