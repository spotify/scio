# Scio, Spark and Scalding

Check out the [Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/) first for a detailed explanation of the Beam programming model and concepts. Also read more about the relationship between [[Scio, Beam and Dataflow]].

Scio's API is heavily influenced by Spark with a lot of ideas from Scalding.

## Scio and Spark

The Dataflow programming model is fundamentally different from that of Spark. Read this Google [blog article](https://cloud.google.com/dataflow/blog/dataflow-beam-and-spark-comparison) for more details.

The Scio API is heavily influenced by Spark but there are some minor differences.

- @scaladoc[`SCollection`](com.spotify.scio.values.SCollection) is equivalent to Spark's [`RDD`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD).
- @scaladoc[`PairSCollectionFunctions`](com.spotify.scio.values.PairSCollectionFunctions) and @scaladoc[`DoubleSCollectionFunctions`](com.spotify.scio.values.DoubleSCollectionFunctions) are specialized versions of `SCollection` and equivalent to Spark's [`PairRDDFunctions`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions) and [`DoubleRDDFunctions`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.DoubleRDDFunctions).
- Execution planning is static and happens before the job is submitted. There is no driver node in a Dataflow cluster and one can only perform the equivalent of Spark [_transformations_](http://spark.apache.org/docs/latest/programming-guide.html#transformations) (`RDD` &rarr; `RDD`) but not [_actions_](http://spark.apache.org/docs/latest/programming-guide.html#actions) (`RDD` &rarr; driver local memory).
- There is no [_broadcast_](http://spark.apache.org/docs/latest/programming-guide.html#broadcast-variables) either but the pattern of `RDD` &rarr; driver via _action_ and driver &rarr; `RDD` via _broadcast_ can be replaced with `SCollection.asSingletonSideInput` and `SCollection.withSideInputs`.
- There is no [`DStream`](https://spark.apache.org/docs/latest/streaming-programming-guide.html#discretized-streams-dstreams) (continuous series of `RDD`s) like in Spark Streaming. Values in a `SCollection` are windowed based on timestamp and windowing operation. The same API works regardless of batch (single global window by default) or streaming mode. Aggregation type _transformations_ that produce `SCollection`s of a single value under global window will produce one value each window when a non-global window is defined.
- `SCollection` has extra methods for side input, side output, and windowing.

## Scio and Scalding

Scio has a much simpler abstract data types compared to Scalding.

- Scalding has many abstract data types like [`TypedPipe`](https://twitter.github.io/scalding/api/#com.twitter.scalding.typed.TypedPipe), [`Grouped`](https://twitter.github.io/scalding/api/index.html#com.twitter.scalding.typed.Grouped), [`CoGrouped`](https://twitter.github.io/scalding/api/index.html#com.twitter.scalding.typed.CoGrouped), [`SortedGrouped`](https://twitter.github.io/scalding/api/index.html#com.twitter.scalding.typed.SortedGrouped).
- Many of them are intermediate and enable some optimizations or wrap around [Cascading](http://www.cascading.org/)'s data model.
- As a result many Scalding operations are lazily evaluated, for example in `pipe.groupBy(keyFn).reduce(mergeFn)`, `mergeFn` is lifted into `groupBy` to operate on the map side as well.
- Scio on the other hand, has only one main data type `SCollection[T]` and `SCollection[(K, V)]` is a specialized variation when the elements are key-value pairs.
- All Scio operations are strictly evaluated, for example `p.groupBy(keyFn)` returns `(K, Iterable[T])` where the values are immediately grouped, whereas `p.reduceByKey(_ + _)` groups `(K, V)` pairs on `K` and reduces values.

Some features may look familiar to Scalding users.

- @scaladoc[`Args`](com.spotify.scio.Args) is a simple command line argument parser similar to the one in Scalding.
- Powerful transforms are possible with `sum`, `sumByKey`, `aggregate`, `aggregrateByKey` using [Algebird](https://github.com/twitter/algebird) `Semigroup`s and `Aggregator`s.
- @scaladoc[`MultiJoin`](com.spotify.scio.util.MultiJoin$) and coGroup of up to 22 sources.
- @scaladoc[`JobTest`](com.spotify.scio.testing.JobTest$) for end to end pipeline testing.

## SCollection

`SCollection` has a few variations.

- @scaladoc[`SCollectionWithSideInput`](com.spotify.scio.values.SCollectionWithSideInput) for replicating small `SCollection`s to all left-hand side values in a large `SCollection`.
- @scaladoc[`SCollectionWithSideOutput`](com.spotify.scio.values.SCollectionWithSideOutput) for output to multiple SCollections.
- @scaladoc[`WindowedSCollection`](com.spotify.scio.values.WindowedSCollection) for accessing window information.
- @scaladoc[`SCollectionWithFanout`](com.spotify.scio.values.SCollectionWithFanout) and @scaladoc[`SCollectionWithHotKeyFanout`](com.spotify.scio.values.SCollectionWithHotKeyFanout) for fanout of skewed data.

## Additional features

Scio also offers some additional features.

- Each worker can pull files from Google Cloud Storage via @scaladoc[`DistCache`](com.spotify.scio.values.DistCache) to be used in transforms locally, similar to Hadoop distributed cache. See @extref[DistCacheExample.scala](example:DistCacheExample).
- Type safe BigQuery IO via Scala macros. Case classes and converters are generated at compile time based on BQ schema. This eliminates the error prone process of handling generic JSON objects. See @extref[TypedBigQueryTornadoes.scala](example:TypedBigQueryTornadoes).
- Sinks (`saveAs*` methods) return `ClosedTap[T]` that can be opened either in another pipeline as `SCollection[T]` or directly as `Iterator[T]` once the current pipeline completes. This enables complex pipeline orchestration. See @extref[WordCountOrchestration.scala](example:WordCountOrchestration).
