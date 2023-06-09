# Built-in Functionality

Scio is a thin wrapper on top of Beam offering idiomatic Scala style API. Check out the [Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/) first for a detailed explanation of the Beam programming model and concepts.

## Basics

- @scaladoc[`ScioContext`](com.spotify.scio.ScioContext) wraps Beam's @javadoc[`Pipeline`](org.apache.beam.sdk.Pipeline)
- @scaladoc[`SCollection`](com.spotify.scio.values.SCollection) wraps Beam's @javadoc[`PCollection`](org.apache.beam.sdk.values.PCollection)
- @scaladoc[`ScioResult`](com.spotify.scio.ScioResult) wraps Beam's @javadoc[`PipelineResult`](org.apache.beam.sdk.PipelineResult)

See dedicated sections on:
- @ref[IO](io/index.md)
- @ref[Joins](Joins.md)
- @ref[Side Inputs](SideInputs.md)

## Core functionality

A `ScioContext` represents the pipeline and is the start point for performing reads and the means by which the pipeline is executed.
Run a pipeline by calling `run` and await completion by chaining `waitUntilDone`:

```scala mdoc:compile-only
import com.spotify.scio._
val sc: ScioContext = ???
sc.run().waitUntilDone()
```

`SCollection` is the representation of the data in a pipeline at a particular point in the execution graph preceding or following a transform.
`SCollection`s have many of the methods you would expect on a standard scala collection: `map`, `filter`, `flatten`, `flatMap`, `reduce`, `collect`, `fold`, and `take`.

Any `SCollection` of 2-tuples is considered a _keyed_ `SCollection` and the various @ref[joins](Joins.md) and `*ByKey` variants of other methods become available.
The first item in the tuple is considered the key and the second item the value.
The `keyBy` method creates a keyed `SCollection`, where the user-defined function extracts the key from the exising values:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val elements: SCollection[String] = ???
val result: SCollection[(String, String)] = elements.keyBy(_.head.toString)
```

Once keyed, elements with the same key can be grouped so that they can be processed together:
```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val elements: SCollection[(String, String)] = ???
val result: SCollection[(String, Iterable[String])] = elements.groupByKey
```

Distinct elements can be found with `distinct` (or the `distinctBy` and `distinctByKey` variants):
```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val elements: SCollection[String] = ???
val distinct: SCollection[String] = elements.distinct
```

Elements can be split into different `SCollection`s with `partition`, which can be useful for error handling.
Note that the number of partitions should be small.
```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val elements: SCollection[Int] = ???
val (lessThanFive, greaterThanFive): (SCollection[Int], SCollection[Int]) = elements.partition(_ > 5)
```

Elements can be printed to the console for inspection at any point of the graph by using `debug`:
```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val elements: SCollection[String] = ???
elements.debug(prefix = "myLabel: ")
```

## ContextAndArgs

Scio's @scaladoc[ContextAndArgs](com.spotify.scio.ContextAndArgs) provides a convenient way to both parse command-line options and acquire a `ScioContext`:

```scala mdoc:compile-only
import com.spotify.scio._

val cmdlineArgs: Array[String] = ???
val (sc, args) = ContextAndArgs(cmdlineArgs)
```

If you need custom pipeline options, subclass Beam's @javadoc[PipelineOptions](org.apache.beam.sdk.options.PipelineOptions) and use `ContextAndArgs.typed`:

```scala mdoc:compile-only
import com.spotify.scio._
import org.apache.beam.sdk.options.PipelineOptions

trait Arguments extends PipelineOptions {
  def getMyArg: String
  def setMyArg(input: String): Unit
}

val cmdlineArgs: Array[String] = ???
val (sc, args) = ContextAndArgs.typed[Arguments](cmdlineArgs)
val myArg: String = args.getMyArg
```

## Aggregations

Scio provides a suite of built-in aggregations

`aggregate`
`aggregateByKey`
`reduce`
`reduceByKey`
`combine`
`combineByKey`

`sum`
`sumByKey`
`count`
`countByKey`
`countApproxDistinct`
`countApproxDistinctByKey`
`countByValue`
`min`
`minByKey`
`max`
`maxByKey`
`mean`
`quantilesApprox`
`approxQuantilesByKey`


See also @ref[Algebird](extras/Algebird.md) for information about Monoid and Semigroup-backed aggregations.

For `SCollection`s containing `Double`, Scio additionally provides a @scaladoc[`stats`](com.spotify.scio.values.DoubleSCollectionFunctions#stats:com.spotify.scio.values.SCollection[com.spotify.scio.util.StatCounter]) method that computes the count, mean, min, max, variance, standard deviation, sample variance, and sample standard deviation over the `SCollection`.
Convenience methods are available directly on the @scaladoc[`SCollection`](com.spotify.scio.values.DoubleSCollectionFunctions) if only a single value is required:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.util.StatCounter

val elements: SCollection[Double] = ???

val stats: SCollection[StatCounter] = elements.stats

val variance: SCollection[Double] = stats.map { s => s.variance }
val stdev: SCollection[Double] = elements.stdev
```

## Metrics

Scio supports Beam's @javadoc[Counter](org.apache.beam.sdk.metrics.Counter) @javadoc[Distribution](org.apache.beam.sdk.metrics.Distribution) and @javadoc[Gauge](org.apache.beam.sdk.metrics.Gauge).

See @extref[MetricsExample](example:MetricsExample).

## ScioResult

@scaladoc[ScioResult](com.spotify.scio.ScioResult) can be used to access metric values, individually or as a group:

```scala mdoc:compile-only
import com.spotify.scio._
import org.apache.beam.sdk.metrics.{MetricName, Counter}

val sc: ScioContext = ???
val counter: Counter = ???

val sr: ScioResult = sc.run().waitUntilDone()
val counterValue: metrics.MetricValue[Long] = sr.counter(counter)
val counterMap: Map[MetricName, metrics.MetricValue[Long]] = sr.allCounters
```

## Taps & Materialization

Writes return a @scaladoc[`ClosedTap`](com.spotify.scio.io.ClosedTap), which provides an interface to access the written results or pass them to a subsequent Scio job.

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.io.{Tap, ClosedTap}
import com.spotify.scio.values.SCollection

val sc: ScioContext = ???
val elements: SCollection[String] = ???
val writeTap: ClosedTap[String] = elements.saveAsTextFile("gs://output-path")

val sr: ScioResult = sc.run().waitUntilDone()

val textTap: Tap[String] = sr.tap(writeTap)
val textContexts: Iterator[String] = textTap.value

val sc2: ScioContext = ???
val results: SCollection[String] = textTap.open(sc)
```

The same mechanism underlies Scio's @scaladoc[`materialize`](com.spotify.scio.values.SCollection#materialize:com.spotify.scio.io.ClosedTap[T]) method, which will save the contents of an `SCollection` at the point of the `materialize` to a temporary location and make them available after the pipeline completes:

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.io.{Tap, ClosedTap}
import com.spotify.scio.values.SCollection

val sc: ScioContext = ???
val elements: SCollection[String] = ???
val materializeTap: ClosedTap[String] = elements.materialize

val sr: ScioResult = sc.run().waitUntilDone()
val textTap: Tap[String] = sr.tap(materializeTap)
```

See also: @extref[WordCountOrchestration](example:WordCountOrchestration) example.

## Use native Beam functionality

If there is a need to use a Beam IO or transform for which Scio does not have an API, you can easily use the native Beam API for single steps in a pipeline otherwise written in Scio.

@scaladoc[`customInput`](com.spotify.scio.ScioContext#customInput[T,I%3E:org.apache.beam.sdk.values.PBegin%3C:org.apache.beam.sdk.values.PInput](name:String,transform:org.apache.beam.sdk.transforms.PTransform[I,org.apache.beam.sdk.values.PCollection[T]]):com.spotify.scio.values.SCollection[T]) supports reading from a Beam source; any transform of type `PTransform[PBegin, PCollection[T]]`:

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.{PBegin, PCollection}
import org.apache.beam.sdk.io.TextIO

val sc: ScioContext = ???
val filePattern: String = ???

val textRead: PTransform[PBegin, PCollection[String]] = TextIO.read().from(filePattern)
val elements: SCollection[String] = sc.customInput("ReadText", textRead)
```

@scaladoc[`saveAsCustomOutput`](com.spotify.scio.values.SCollection#saveAsCustomOutput[O%3C:org.apache.beam.sdk.values.POutput](name:String,transform:org.apache.beam.sdk.transforms.PTransform[org.apache.beam.sdk.values.PCollection[T],O]):com.spotify.scio.io.ClosedTap[Nothing]) supports writing to a Beam sink; any transform of type `PTransform[PCollection[T], PDone]`:

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.{PDone, PCollection}
import org.apache.beam.sdk.io.TextIO

val outputLocation: String = ???
val elements: SCollection[String] = ???
val textWrite: PTransform[PCollection[String], PDone] = TextIO.write().to(outputLocation)
elements.saveAsCustomOutput("WriteText", textWrite)
```

Finally, @scaladoc[`applyTransform`](com.spotify.scio.values.SCollection#applyTransform[U](transform:org.apache.beam.sdk.transforms.PTransform[_%3E:org.apache.beam.sdk.values.PCollection[T],org.apache.beam.sdk.values.PCollection[U]])(implicitevidence$2:com.spotify.scio.coders.Coder[U]):com.spotify.scio.values.SCollection[U]) supports using any Beam transform of type `PTransform[PCollection[T], PCollection[U]]`:

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.{PTransform, Sum}
import org.apache.beam.sdk.values.{PDone, PCollection}
import java.lang

val elements: SCollection[Double] = ???
val transform: PTransform[PCollection[lang.Double], PCollection[lang.Double]] = Sum.doublesGlobally
val result: SCollection[lang.Double] = elements
  .map(Double.box)
  .applyTransform(transform)
```

See also: @extref[BeamExample](example:BeamExample)

## Windowing & streaming

TODO
`toWindowed`
`withWindowFn`
`withFixedWindows`
`withSlidingWindows`
`withSessionWindows`
`withGlobalWindow`
`withPaneInfo`
`withTimestamp`
`timestampBy`
`withWindow`

## Batching

In cases where some transform performs better on a group of items, elements can be batched by number of elements with [`batch`](com.spotify.scio.values.SCollection#batch(batchSize:Long,maxLiveWindows:Int):com.spotify.scio.values.SCollection[Iterable[T]]), by the size of the elements with [`batchByteSized`](com.spotify.scio.values.SCollection#batchByteSized(batchByteSize:Long,maxLiveWindows:Int):com.spotify.scio.values.SCollection[Iterable[T]]), or by some user-defined weight with [`batchWeighted`](com.spotify.scio.values.SCollection#batchWeighted(batchWeight:Long,cost:T=>Long,maxLiveWindows:Int):com.spotify.scio.values.SCollection[Iterable[T]]).
There are also keyed variants of each of these: [`batchByKey`](com.spotify.scio.values.PairSCollectionFunctions#batchByKey(batchSize:Long,maxBufferingDuration:org.joda.time.Duration):com.spotify.scio.values.SCollection[(K,Iterable[V])]), [`batchByteSizedByKey`](com.spotify.scio.values.PairSCollectionFunctions#batchByteSizedByKey(batchByteSize:Long,maxBufferingDuration:org.joda.time.Duration):com.spotify.scio.values.SCollection[(K,Iterable[V])]), and [`batchWeightedByKey`](com.spotify.scio.values.PairSCollectionFunctions#batchWeightedByKey(weight:Long,cost:V=>Long,maxBufferingDuration:org.joda.time.Duration):com.spotify.scio.values.SCollection[(K,Iterable[V])]).

```scala
import com.spotify.scio._
import com.spotify.scio.values.SCollection

val elements: SCollection[String] = ???
val batchedElements: SCollection[Iterable[String]] = elements.batch(10)
```


## Misc


Some elements of an `SCollection` can be randomly sampled using `sample`:
```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val elements: SCollection[String] = ???
val result: SCollection[String] = elements.sample(withReplacement = true, fraction = 0.01)
```


`intersection`
`subtract`
`cross`
`hashLookup`
`reify*`
`randomSplit`
`top`