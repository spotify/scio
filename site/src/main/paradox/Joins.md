# Joins

Scio provides a full suite of join functionality and a few extras that solve tricky edge-cases in large-scale data processing.

All joins operate over `SCollection`s containing 2-tuples, where the first tuple item is considered the _key_ and the second the _value_.
The order in which `SCollection`s are joined matters; larger datasets should be further to the left.
For example in `a.join(b)`, `a` should be the larger of the two datasets and by convention `a` is called the left-hand-side or _LHS_, while `b` is the right-hand-side or _RHS_.

## Cogroup

The Beam transform which underlies the standard joins below is the [Cogroup](https://beam.apache.org/documentation/programming-guide/#cogroupbykey).
Scio also provides a @scaladoc[cogroup](com.spotify.scio.values.PairSCollectionFunctions#cogroup[W](rhs:com.spotify.scio.values.SCollection[(K,W)]):com.spotify.scio.values.SCollection[(K,(Iterable[V],Iterable[W]))]) operation, which returns iterables from each `SCollection` containing all the values which match each key:

```scala
import com.spotify.scio.values.SCollection

val a: SCollection[(String, String)] = ???
val b: SCollection[(String, Int)] = ???
val elements: SCollection[(String, (Iterable[String], Iterable[Int]))] = a.cogroup(b)
```

## Standard joins

Scio's standard joins have SQL-like names with SQL-like semantics.
In the examples below, the contents of the LHS are of type `(K, V)`, while the RHS are of type `(K, W)`.

@scaladoc[join](com.spotify.scio.values.PairSCollectionFunctions#join[W](rhs:com.spotify.scio.values.SCollection[(K,W)]):com.spotify.scio.values.SCollection[(K,(V,W))]) produces elements of `(K, (V, W))`, where the key `K` must be in both the LHS and RHS:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val a: SCollection[(String, String)] = ???
val b: SCollection[(String, Int)] = ???
val elements: SCollection[(String, (String, Int))] = a.join(b)
```

@scaladoc[leftOuterJoin](com.spotify.scio.values.PairSCollectionFunctions#leftOuterJoin[W](rhs:com.spotify.scio.values.SCollection[(K,W)]):com.spotify.scio.values.SCollection[(K,(V,Option[W]))]) produces elements of `(K (V, Option[W]))`, where the key `K` is in the LHS but may not be in the RHS:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val a: SCollection[(String, String)] = ???
val b: SCollection[(String, Int)] = ???
val elements: SCollection[(String, (String, Option[Int]))] = a.leftOuterJoin(b)
```

@scaladoc[rightOuterJoin](com.spotify.scio.values.PairSCollectionFunctions#rightOuterJoin[W](rhs:com.spotify.scio.values.SCollection[(K,W)]):com.spotify.scio.values.SCollection[(K,(Option[V],W))]) produces elements of `(K, (Option[V], W]))`, where the key `K` is in the RHS but may not be in the LHS:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val a: SCollection[(String, String)] = ???
val b: SCollection[(String, Int)] = ???
val elements: SCollection[(String, (Option[String], Int))] = a.rightOuterJoin(b)
```

@scaladoc[fullOuterJoin](com.spotify.scio.values.PairSCollectionFunctions#fullOuterJoin[W](rhs:com.spotify.scio.values.SCollection[(K,W)]):com.spotify.scio.values.SCollection[(K,(Option[V],Option[W]))]) produces elements of `(K (Option[V], Option[W]))`, where the key `K` can be in either side:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val a: SCollection[(String, String)] = ???
val b: SCollection[(String, Int)] = ???
val elements: SCollection[(String, (Option[String], Option[Int]))] = a.fullOuterJoin(b)
```

When multiple joins of the same type are chained, it is more efficient to use Scio's @scaladoc[MultiJoin](com.spotify.scio.util.MultiJoin) class. Instead of `a.join(b).join(c)` prefer `MultiJoin` (or its variants, `MultiJoin.left`, `MultiJoin.outer`, `MultiJoin.cogroup`):

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.util.MultiJoin

val a: SCollection[(String, Int)] = ???
val b: SCollection[(String, Boolean)] = ???
val c: SCollection[(String, Float)] = ???
val elements: SCollection[(String, (Int, Boolean, Float))] = MultiJoin(a, b, c)
```

## Hash joins

Scio's @scaladoc[hashJoin](com.spotify.scio.values.PairHashSCollectionFunctions#hashJoin[W](rhs:com.spotify.scio.values.SCollection[(K,W)]):com.spotify.scio.values.SCollection[(K,(V,W))]) and variants @scaladoc[hashLeftOuterJoin](com.spotify.scio.values.PairHashSCollectionFunctions#hashLeftOuterJoin[W](sideInput:com.spotify.scio.values.SideInput[Map[K,Iterable[W]]]):com.spotify.scio.values.SCollection[(K,(V,Option[W]))]), and @scaladoc[hashFullOuterJoin](com.spotify.scio.values.PairHashSCollectionFunctions#hashFullOuterJoin[W](rhs:com.spotify.scio.values.SCollection[(K,W)]):com.spotify.scio.values.SCollection[(K,(Option[V],Option[W]))]) provide a convenient syntax over the top of Beam's SideInput class to avoid shuffle during the join.
The RHS should fit in memory, as with normal @ref[SideInputs](SideInputs.md).

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val a: SCollection[(String, Int)] = ???
val b: SCollection[(String, Boolean)] = ???
val elements: SCollection[(String, (Int, Boolean))] = a.hashJoin(b)
```

In the less-common case where the LHS contains only keys to be looked-up, @scaladoc[hashLookup](com.spotify.scio.values.SCollection#hashLookup[V](that:com.spotify.scio.values.SCollection[(T,V)]):com.spotify.scio.values.SCollection[(T,Iterable[V])]) will join in all matching values from the RHS.
Again, the RHS should fit in memory.

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val a: SCollection[String] = ???
val b: SCollection[(String, String)] = ???
val elements: SCollection[(String, Iterable[String])] = a.hashLookup(b)
```

In addition, Scio also provides the shuffle-free intersection and subtraction operations @scaladoc[hashIntersectByKey](com.spotify.scio.values.PairHashSCollectionFunctions#hashIntersectByKey(rhs:com.spotify.scio.values.SCollection[K]):com.spotify.scio.values.SCollection[(K,V)]) and @scaladoc[hashSubtractByKey](com.spotify.scio.values.PairHashSCollectionFunctions#hashSubtractByKey(sideInput:com.spotify.scio.values.SideInput[Set[K]]):com.spotify.scio.values.SCollection[(K,V)]).

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val a: SCollection[(String, Int)] = ???
val b: SCollection[String] = ???

val subtracted: SCollection[(String, Int)] = a.hashSubtractByKey(b)
val intersected: SCollection[(String, Int)] = a.hashIntersectByKey(b)
```

## Large hash join

Similar to Hash Joins, Scio's @scaladoc[largeHashJoin](com.spotify.scio.extra.sparkey.PairLargeHashSCollectionFunctions#largeHashJoin[W](rhs:com.spotify.scio.values.SCollection[(K,W)],numShards:Short,compressionType:com.spotify.sparkey.CompressionType,compressionBlockSize:Int):com.spotify.scio.values.SCollection[(K,(V,W))]) and variants @scaladoc[largeHashLeftOuterJoin](com.spotify.scio.extra.sparkey.PairLargeHashSCollectionFunctions#largeHashLeftOuterJoin[W](rhs:com.spotify.scio.values.SCollection[(K,W)],numShards:Short,compressionType:com.spotify.sparkey.CompressionType,compressionBlockSize:Int):com.spotify.scio.values.SCollection[(K,(V,Option[W]))]) and @scaladoc[largeHashFullOuterJoin](com.spotify.scio.extra.sparkey.PairLargeHashSCollectionFunctions#largeHashFullOuterJoin[W](rhs:com.spotify.scio.values.SCollection[(K,W)],numShards:Short,compressionType:com.spotify.sparkey.CompressionType,compressionBlockSize:Int):com.spotify.scio.values.SCollection[(K,(Option[V],Option[W]))]) provide a convenient syntax on top of Scio's @ref[Sparkey](extras/Sparkey.md) support to avoid shuffle during a join.
Use of sparkey requires only that the RHS fit on disk.

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.sparkey._

val a: SCollection[(String, Int)] = ???
val b: SCollection[(String, Boolean)] = ???
val elements: SCollection[(String, (Int, Boolean))] = a.largeHashJoin(b)
```

Larger shuffle-free intersection and subtraction operations are also provided as @scaladoc[largeHashIntersectByKey](com.spotify.scio.extra.sparkey.PairLargeHashSCollectionFunctions#largeHashIntersectByKey(rhs:com.spotify.scio.values.SCollection[K],numShards:Short,compressionType:com.spotify.sparkey.CompressionType,compressionBlockSize:Int):com.spotify.scio.values.SCollection[(K,V)]) and @scaladoc[largeHashSubtractByKey](com.spotify.scio.extra.sparkey.PairLargeHashSCollectionFunctions#largeHashSubtractByKey(rhs:com.spotify.scio.values.SCollection[K],numShards:Short,compressionType:com.spotify.sparkey.CompressionType,compressionBlockSize:Int):com.spotify.scio.values.SCollection[(K,V)]).

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.sparkey._

val a: SCollection[(String, Int)] = ???
val b: SCollection[String] = ???

val subtracted: SCollection[(String, Int)] = a.largeHashSubtractByKey(b)
val intersected: SCollection[(String, Int)] = a.largeHashIntersectByKey(b)
```

## Sparse join

Scio supports a 'sparse join' for cases where both the LHS and RHS of a join are large, but where the keys in the RHS cover a relatively small number of rows in the LHS.

In this case, an optimization of the join can significantly reduce the shuffle.
The keys of the RHS are inserted into a [Bloom Filter](https://en.wikipedia.org/wiki/Bloom_filter), a probabilistic data structure that effectively acts as a `Set` but with some risk of false positives.
Elements in the LHS dataset are partitioned by passing an element's key through the filter and splitting the dataset on whether the key is found or not.
All LHS keys which are found in the filter are _probably_ in the RHS dataset, so a full join is performed on these elements.
Any LHS key _not found_ in the filter are _definitely not_ in the RHS dataset, so these items can be handled without a join.
To properly size the Bloom filter, an estimate of the number of keys in the RHS (`rhsNumKeys`) must be provided to the join function.

In addition to @scaladoc[sparseJoin](com.spotify.scio.values.PairSCollectionFunctions#sparseJoin[W](rhs:com.spotify.scio.values.SCollection[(K,W)],rhsNumKeys:Long,fpProb:Double)(implicitfunnel:com.google.common.hash.Funnel[K]):com.spotify.scio.values.SCollection[(K,(V,W))]) (and variants @scaladoc[sparseLeftOuterJoin](com.spotify.scio.values.PairSCollectionFunctions#sparseLeftOuterJoin[W](rhs:com.spotify.scio.values.SCollection[(K,W)],rhsNumKeys:Long,fpProb:Double)(implicitfunnel:com.google.common.hash.Funnel[K]):com.spotify.scio.values.SCollection[(K,(V,Option[W]))]), @scaladoc[sparseRightOuterJoin](com.spotify.scio.values.PairSCollectionFunctions#sparseRightOuterJoin[W](rhs:com.spotify.scio.values.SCollection[(K,W)],rhsNumKeys:Long,fpProb:Double)(implicitfunnel:com.google.common.hash.Funnel[K]):com.spotify.scio.values.SCollection[(K,(Option[V],W))]), and @scaladoc[sparseFullOuterJoin](com.spotify.scio.values.PairSCollectionFunctions#sparseFullOuterJoin[W](rhs:com.spotify.scio.values.SCollection[(K,W)],rhsNumKeys:Long,fpProb:Double)(implicitfunnel:com.google.common.hash.Funnel[K]):com.spotify.scio.values.SCollection[(K,(Option[V],Option[W]))])) Scio also provides a @scaladoc[sparseIntersectByKey](com.spotify.scio.values.PairSCollectionFunctions#sparseIntersectByKey(rhs:com.spotify.scio.values.SCollection[K],rhsNumKeys:Long,computeExact:Boolean,fpProb:Double)(implicitfunnel:com.google.common.hash.Funnel[K]):com.spotify.scio.values.SCollection[(K,V)]) implementation.
Scio uses Guava's @javadoc[BloomFilter](com.google.common.hash.BloomFilter).
Import `magnolify.guava.auto._` to get common instances of Guava @javadoc[Funnel](com.google.common.hash.Funnel):

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import magnolify.guava.auto._

val a: SCollection[(String, Int)] = ???
val b: SCollection[(String, Boolean)] = ???
val c: SCollection[String] = ???

val bNumKeys: Int = ???
val joined = a.sparseJoin(b, bNumKeys)

val cNumKeys: Int = ???
val intersected: SCollection[(String, Int)] = a.sparseIntersectByKey(c, cNumKeys)
```

Finally, Scio provides @scaladoc[sparseLookup](com.spotify.scio.values.PairSCollectionFunctions#sparseLookup[A](rhs:com.spotify.scio.values.SCollection[(K,A)],thisNumKeys:Long)(implicitfunnel:com.google.common.hash.Funnel[K]):com.spotify.scio.values.SCollection[(K,(V,Iterable[A]))]), a special-case for joining all items from the RHS with matching keys into the LHS items with that key.
Differently than the other `sparse` variants, in this case an estimate of the number of keys in the LHS must be provided:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import magnolify.guava.auto._

val a: SCollection[(String, Int)] = ???
val b: SCollection[(String, String)] = ???

val aNumKeys: Int = ???
val lookedUp: SCollection[(String, (Int, Iterable[String]))] = a.sparseLookup(b, aNumKeys)
```

## Skewed Join

Similar to sparse joins, Scio supports a 'skewed join' for the special case in which some keys in a dataset are very frequent, or _hot_.

Scio uses a [Count-Min sketch](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) (or _CMS_), a probabilistic data structure that is internally similar to a Bloom filter, but which provides an estimated count for a given item which is explicitly an _over_estimate.
The keys of the LHS are counted and those which exceed the value of the `hotKeyThreshold` parameter (default: `9000`) plus an error bound are considered 'hot', while any remaining key is 'cold'.
Both the LHS and RHS are divided into 'hot' and 'chill' partitions.
The chill sides are joined normally, while the hot side of the RHS is `hashJoin`ed into the hot LHS, avoiding shuffle on this segment of the dataset.

Scio provides @scaladoc[skewedJoin](com.spotify.scio.values.PairSkewedSCollectionFunctions), @scaladoc[skewedLeftOuterJoin](com.spotify.scio.values.PairSkewedSCollectionFunctions), and @scaladoc[skewedFullOuterJoin](com.spotify.scio.values.PairSkewedSCollectionFunctions) variants.
Import `com.twitter.algebird.CMSHasherImplicits._` for the implicits required for count-min sketch.

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.twitter.algebird.CMSHasherImplicits._

val a: SCollection[(String, Int)] = ???
val b: SCollection[(String, String)] = ???
val elements: SCollection[(String, (Int, String))] = a.skewedJoin(b)
```

## Sort Merge Bucket (SMB)

Sort-Merge Buckets allow for shuffle-free joins of large datasets.
See @ref[Sort Merge Bucket](extras/Sort-Merge-Bucket.md)

## See also

* [Join Optimizations at Spotify](https://youtu.be/cGvaQp_h5ek?t=6257) How Scio can save you time and money with clever join strategies and approximate algorithms, Apache Beam Summit 2022
