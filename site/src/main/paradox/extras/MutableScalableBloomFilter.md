# MutableScalableBloomFilter

Scio ships with an implementation of a scalable Bloom Filter, as described in ["Scalable Bloom Filters", Almeida, Baquero, et al.](http://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf).

A Bloom filter is an _approximate_ data structure that behaves in a similar way to a `Set` and can answer the question "does this set _probably_ contain this value?".

As an example, if you want to be able to answer the question "Does this user listen to Beyonce?" you could construct a `Set` containing all the ids of all the users that listened to Beyonce, persist it, the do a lookup into the set every time you need to know.
The issue is that the `Set` will quickly become very large, especially for such a popular artist, and if you want to maintain many such sets, you will run into scaling issues.
Bloom filters solve this problem by accepting some false positives for significant compression.

A _scalable_ Bloom filter is a sequence of Bloom filters that are iteratively added-to once each constituent filter nears its capacity (the point at which false positive guarantees break down).
This is useful because inputs to a Bloom filter are lost, and it is not possible to resize a filter once constructed.
The [MutableScalableBloomFilter](com.spotify.scio.hash.MutableScalableBloomFilter) implementation shipping with scio maintains some additional metadata which allows it to scale automatically when necessary.

See the @scaladoc[MutableScalableBloomFilter](com.spotify.scio.hash.MutableScalableBloomFilter$) for details on how to properly size a scalable Bloom filter.

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.hash.MutableScalableBloomFilter
import magnolify.guava.auto._

case class TrackListen(trackId: String, userId: String)

val elements: SCollection[TrackListen] = ???
val msbfs: SCollection[MutableScalableBloomFilter[String]] = elements
  .map { t => t.trackId -> t.userId }
  .groupByKey
  .map { case (trackId, userIds) =>
    val msbf = MutableScalableBloomFilter[String](1_000_000)
    msbf ++= userIds
    msbf
  }
```
