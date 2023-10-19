# Built-in Functionality

Scio is a thin wrapper on top of Beam offering idiomatic Scala APIs. Check out the [Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/) first for a detailed explanation of the Beam programming model and concepts.

## Basics

- @scaladoc[ScioContext](com.spotify.scio.ScioContext) wraps Beam's @javadoc[Pipeline](org.apache.beam.sdk.Pipeline)
- @scaladoc[SCollection](com.spotify.scio.values.SCollection) wraps Beam's @javadoc[PCollection](org.apache.beam.sdk.values.PCollection)
- @scaladoc[ScioResult](com.spotify.scio.ScioResult) wraps Beam's @javadoc[PipelineResult](org.apache.beam.sdk.PipelineResult)

See dedicated sections on:
- @ref[IO](io/index.md)
- @ref[Joins](Joins.md)
- @ref[Side Inputs](SideInputs.md)

## Core functionality

A `ScioContext` represents the pipeline and is the starting point for performing reads and the means by which the pipeline is executed.
Execute a pipeline by invoking @scaladoc[run](com.spotify.scio.ScioContext#run():com.spotify.scio.ScioExecutionContext) and await completion by chaining @scaladoc[waitUntilDone](com.spotify.scio.ScioExecutionContext#waitUntilDone(duration:scala.concurrent.duration.Duration,cancelJob:Boolean):com.spotify.scio.ScioResult):

```scala mdoc:compile-only
import com.spotify.scio._
val sc: ScioContext = ???
sc.run().waitUntilDone()
```

`SCollection` is the representation of the data in a pipeline at a particular point in the execution graph preceding or following a transform.
`SCollection`s have many of the methods you would expect on a standard Scala collection: `map`, `filter`, `flatten`, `flatMap`, `reduce`, `collect`, `fold`, and `take`.

Any `SCollection` of 2-tuples is considered a _keyed_ `SCollection` and the various @ref[joins](Joins.md) and `*ByKey` variants of other methods become available.
The first item in the tuple is considered the key and the second item the value.
The @scaladoc[keyBy](com.spotify.scio.values.SCollection#keyBy[K](f:T=%3EK)(implicitevidence$22:com.spotify.scio.coders.Coder[K]):com.spotify.scio.values.SCollection[(K,T)]) method creates a keyed `SCollection`, where the user-defined function extracts the key from the existing values:

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

Distinct elements can be found with @scaladoc[distinct](com.spotify.scio.values.SCollection#distinct:com.spotify.scio.values.SCollection[T]) (or the `distinctBy` and `distinctByKey` variants):

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val elements: SCollection[String] = ???
val distinct: SCollection[String] = elements.distinct
```

Elements can be split into different `SCollection`s with @scaladoc[partition](com.spotify.scio.values.SCollection#partition(p:T=%3EBoolean):(com.spotify.scio.values.SCollection[T],com.spotify.scio.values.SCollection[T])), which can be useful for error handling.
Note that the number of partitions should be small.

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val elements: SCollection[Int] = ???
val (lessThanFive, greaterThanFive): (SCollection[Int], SCollection[Int]) = elements.partition(_ > 5)
```

`SCollection`s of the same type can be combined with a @scaladoc[union](com.spotify.scio.values.SCollection#union(that:com.spotify.scio.values.SCollection[T]):com.spotify.scio.values.SCollection[T]) (or
@scaladoc[unionAll](com.spotify.scio.ScioContext#unionAll[T](scs:=%3EIterable[com.spotify.scio.values.SCollection[T]])(implicitevidence$6:com.spotify.scio.coders.Coder[T]):com.spotify.scio.values.SCollection[T])) operation.

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val a: SCollection[Int] = ???
val b: SCollection[Int] = ???
val elements: SCollection[Int] = a.union(b)
```

Elements can be printed to the console for inspection at any point of the graph by using @scaladoc[debug](com.spotify.scio.values.SCollection#debug(out:()=%3Ejava.io.PrintStream,prefix:String,enabled:Boolean):com.spotify.scio.values.SCollection[T]):

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

Scio provides a suite of built-in aggregations. 
All `*ByKey` variants do the same as the normal function, but per-key for keyed `SCollection`s.

### Counting

* @scaladoc[count](com.spotify.scio.values.SCollection#count:com.spotify.scio.values.SCollection[Long]) (or `countByKey`) counts the number of elements
* @scaladoc[countByValue](com.spotify.scio.values.SCollection#countByValue:com.spotify.scio.values.SCollection[(T,Long)]) counts the number of elements for each value in a `SCollection[T]`
* @scaladoc[countApproxDistinct](com.spotify.scio.values.SCollection#countApproxDistinct(estimator:com.spotify.scio.estimators.ApproxDistinctCounter[T]):com.spotify.scio.values.SCollection[Long]) (or `countApproxDistinctByKey`) estimates a distinct count, with Beam's @javadoc[ApproximateUnique](org.apache.beam.sdk.transforms.ApproximateUnique) or Scio's HyperLogLog-based @scaladoc[ApproxDistinctCounter](com.spotify.scio.estimators.ApproxDistinctCounter)

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.hll.zetasketch.ZetaSketchHllPlusPlus

val elements: SCollection[String] = ???
val sketch = ZetaSketchHllPlusPlus[String]()
val result: SCollection[Long] = elements.countApproxDistinct(sketch)
```

### Statistics

* @scaladoc[max](com.spotify.scio.values.SCollection#max(implicitord:Ordering[T]):com.spotify.scio.values.SCollection[T]) (or `maxByKey`) finds the maximum element given some @scaladoc[Ordering](scala.math.Ordering)
* @scaladoc[min](com.spotify.scio.values.SCollection#min(implicitord:Ordering[T]):com.spotify.scio.values.SCollection[T]) (or `minByKey`) finds the minimum element given some @scaladoc[Ordering](scala.math.Ordering)
* @scaladoc[mean](com.spotify.scio.values.SCollection#mean(implicitev:Numeric[T]):com.spotify.scio.values.SCollection[Double]) finds the mean given some @scaladoc[Numeric](scala.math.Numeric)
* @scaladoc[quantilesApprox](com.spotify.scio.values.SCollection#quantilesApprox(numQuantiles:Int)(implicitord:Ordering[T]):com.spotify.scio.values.SCollection[Iterable[T]]) (or `approxQuantilesByKey`) finds the distribution using Beam's @javadoc[ApproximateQuantiles](org.apache.beam.sdk.transforms.ApproximateQuantiles)

For `SCollection`s containing `Double`, Scio additionally provides a @scaladoc[stats](com.spotify.scio.values.DoubleSCollectionFunctions#stats:com.spotify.scio.values.SCollection[com.spotify.scio.util.StatCounter]) method that computes the count, mean, min, max, variance, standard deviation, sample variance, and sample standard deviation over the `SCollection`.
Convenience methods are available directly on the @scaladoc[SCollection](com.spotify.scio.values.DoubleSCollectionFunctions) if only a single value is required:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.util.StatCounter

val elements: SCollection[Double] = ???

val stats: SCollection[StatCounter] = elements.stats
val variance: SCollection[Double] = stats.map { s => s.variance }

val stdev: SCollection[Double] = elements.stdev
```

### Sums & combinations

@scaladoc[combine](com.spotify.scio.values.SCollection#combine[C](createCombiner:T=%3EC)(mergeValue:(C,T)=%3EC)(mergeCombiners:(C,C)=%3EC)(implicitevidence$15:com.spotify.scio.coders.Coder[C]):com.spotify.scio.values.SCollection[C]) (or `combineByKey`) combines elements with a set of user-defined functions:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

case class A(count: Long, total: Long)
object A {
  def apply(i: Int): A = A(1L, i)
  def mergeValue(a: A, i: Int): A = A(a.count + 1L, a.total + i)
  def mergeCombiners(a: A, b: A) = A(a.count + b.count, a.total + b.total)
}

val elements: SCollection[Int] = ???
elements.combine(A.apply)(A.mergeValue)(A.mergeCombiners)
```

@scaladoc[sum](com.spotify.scio.values.SCollection#sum(implicitsg:com.twitter.algebird.Semigroup[T]):com.spotify.scio.values.SCollection[T]) (or `sumByKey`) sums elements given a @scaladoc[Semigroup](com.twitter.algebird.Semigroup), while @scaladoc[aggregate](com.spotify.scio.values.SCollection#aggregate[A,U](aggregator:com.twitter.algebird.MonoidAggregator[T,A,U])(implicitevidence$12:com.spotify.scio.coders.Coder[A],implicitevidence$13:com.spotify.scio.coders.Coder[U]):com.spotify.scio.values.SCollection[U]) (or `aggregateByKey`) aggregates elements either with a set of user-defined functions, via a @scaladoc[Aggregator](com.twitter.algebird.Aggregator), or via a @scaladoc[MonoidAggregator](com.twitter.algebird.MonoidAggregator).

Both `Semigroup` and `Monoid` instances can be derived with [magnolify](https://github.com/spotify/magnolify/blob/main/docs/derivation.md), assuming the behavior for the primitive types is what you expect.

@@@ note

Note that for `String` the default `Semigroup[String]` behavior is to append, which is usually not what you want.

@@@

Fully-automatic derivation can be very concise but relies on some implicit magic:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.twitter.algebird._
import magnolify.cats.auto._
import cats._

case class A(count: Long, total: Long)

val elements: SCollection[A] = ???

val summed: SCollection[A] = elements.sum
val aggregated: SCollection[A] = elements.aggregate(Aggregator.fromMonoid[A])
```

Semi-automatic derivation in a companion object may be more intelligible:

```scala mdoc:compile-only
case class A(count: Long, total: Long)
object A {
  import magnolify.cats.semiauto._
  import cats._
  implicit val aMonoid: cats.Monoid[A] = MonoidDerivation[A]
}
```

See also @ref[Algebird](extras/Algebird.md)

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

Writes return a @scaladoc[ClosedTap](com.spotify.scio.io.ClosedTap), which provides an interface to access the written results or pass them to a subsequent Scio job.

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

The same mechanism underlies Scio's @scaladoc[materialize](com.spotify.scio.values.SCollection#materialize:com.spotify.scio.io.ClosedTap[T]) method, which will save the contents of an `SCollection` at the point of the `materialize` to a temporary location and make them available after the pipeline completes:

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

@scaladoc[customInput](com.spotify.scio.ScioContext#customInput[T,I%3E:org.apache.beam.sdk.values.PBegin%3C:org.apache.beam.sdk.values.PInput](name:String,transform:org.apache.beam.sdk.transforms.PTransform[I,org.apache.beam.sdk.values.PCollection[T]]):com.spotify.scio.values.SCollection[T]) supports reading from a Beam source; any transform of type `PTransform[PBegin, PCollection[T]]`:

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

@scaladoc[saveAsCustomOutput](com.spotify.scio.values.SCollection#saveAsCustomOutput[O%3C:org.apache.beam.sdk.values.POutput](name:String,transform:org.apache.beam.sdk.transforms.PTransform[org.apache.beam.sdk.values.PCollection[T],O]):com.spotify.scio.io.ClosedTap[Nothing]) supports writing to a Beam sink; any transform of type `PTransform[PCollection[T], PDone]`:

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

Finally, @scaladoc[applyTransform](com.spotify.scio.values.SCollection#applyTransform[U](transform:org.apache.beam.sdk.transforms.PTransform[_%3E:org.apache.beam.sdk.values.PCollection[T],org.apache.beam.sdk.values.PCollection[U]])(implicitevidence$2:com.spotify.scio.coders.Coder[U]):com.spotify.scio.values.SCollection[U]) supports using any Beam transform of type `PTransform[PCollection[T], PCollection[U]]`:

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

## Windowing

@scaladoc[timestampBy](com.spotify.scio.values.SCollection#timestampBy(f:T=%3Eorg.joda.time.Instant,allowedTimestampSkew:org.joda.time.Duration):com.spotify.scio.values.SCollection[T]) allows for changing an element's timestamp:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import org.joda.time.Instant

case class A(timestamp: Instant, value: String)
val elements: SCollection[A] = ???
val timestamped: SCollection[A] = elements.timestampBy(_.timestamp)
```

The @scaladoc[withTimestamp](com.spotify.scio.values.SCollection#withTimestamp:com.spotify.scio.values.SCollection[(T,org.joda.time.Instant)]), @scaladoc[withWindow](com.spotify.scio.values.SCollection#withWindow[W%3C:org.apache.beam.sdk.transforms.windowing.BoundedWindow]:com.spotify.scio.values.SCollection[(T,W)]), and @scaladoc[withPaneInfo](com.spotify.scio.values.SCollection#withPaneInfo:com.spotify.scio.values.SCollection[(T,org.apache.beam.sdk.transforms.windowing.PaneInfo)]) functions flatten window metadata into the `SCollection`:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import org.joda.time.Instant

val elements: SCollection[String] = ???
val timestamped: SCollection[(String, Instant)] = elements.withTimestamp
```

@scaladoc[toWindowed](com.spotify.scio.values.SCollection#toWindowed:com.spotify.scio.values.WindowedSCollection[T]) converts the `SCollection` to a @scaladoc[WindowedSCollection](com.spotify.scio.values.WindowedSCollection) whose elements are all instances of @scaladoc[WindowedValue](com.spotify.scio.values.WindowedValue), which gives full access to the windowing metadata:

```scala mdoc:compile-only
import com.spotify.scio.values._
import org.joda.time.Instant

val elements: SCollection[String] = ???
val windowed: WindowedSCollection[String] = elements.toWindowed
windowed.map { v: WindowedValue[String] =>
  v.withTimestamp(Instant.now())
}
```

Scio provides convenience functions for the common types of windowing (@scaladoc[withFixedWindows](com.spotify.scio.values.SCollection#withFixedWindows(duration:org.joda.time.Duration,offset:org.joda.time.Duration,options:com.spotify.scio.values.WindowOptions):com.spotify.scio.values.SCollection[T]), @scaladoc[withSlidingWindows](com.spotify.scio.values.SCollection#withSlidingWindows(size:org.joda.time.Duration,period:org.joda.time.Duration,offset:org.joda.time.Duration,options:com.spotify.scio.values.WindowOptions):com.spotify.scio.values.SCollection[T]), @scaladoc[withSessionWindows](com.spotify.scio.values.SCollection#withSessionWindows(gapDuration:org.joda.time.Duration,options:com.spotify.scio.values.WindowOptions):com.spotify.scio.values.SCollection[T]), @scaladoc[withGlobalWindow](com.spotify.scio.values.SCollection#withGlobalWindow(options:com.spotify.scio.values.WindowOptions):com.spotify.scio.values.SCollection[T])) but also provides full control over the windowing with @scaladoc[withWindowFn](com.spotify.scio.values.SCollection#withWindowFn[W%3C:org.apache.beam.sdk.transforms.windowing.BoundedWindow](fn:org.apache.beam.sdk.transforms.windowing.WindowFn[_,W],options:com.spotify.scio.values.WindowOptions):com.spotify.scio.values.SCollection[T]).

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import org.joda.time.Duration

val elements: SCollection[String] = ???
val windowedElements: SCollection[String] = elements.withFixedWindows(Duration.standardHours(1))
```

## Batching

In cases where some transform performs better on a group of items, elements can be batched by number of elements with @scaladoc[`batch`](com.spotify.scio.values.SCollection#batch(batchSize:Long,maxLiveWindows:Int):com.spotify.scio.values.SCollection[Iterable[T]]), by the size of the elements with @scaladoc[`batchByteSized`](com.spotify.scio.values.SCollection#batchByteSized(batchByteSize:Long,maxLiveWindows:Int):com.spotify.scio.values.SCollection[Iterable[T]]), or by some user-defined weight with @scaladoc[`batchWeighted`](com.spotify.scio.values.SCollection#batchWeighted(batchWeight:Long,cost:T=%3ELong,maxLiveWindows:Int):com.spotify.scio.values.SCollection[Iterable[T]]).
There are also keyed variants of each of these: @scaladoc[`batchByKey`](com.spotify.scio.values.PairSCollectionFunctions#batchByKey(batchSize:Long,maxBufferingDuration:org.joda.time.Duration):com.spotify.scio.values.SCollection[(K,Iterable[V])]), @scaladoc[`batchByteSizedByKey`](com.spotify.scio.values.PairSCollectionFunctions#batchByteSizedByKey(batchByteSize:Long,maxBufferingDuration:org.joda.time.Duration):com.spotify.scio.values.SCollection[(K,Iterable[V])]), and @scaladoc[`batchWeightedByKey`](com.spotify.scio.values.PairSCollectionFunctions#batchWeightedByKey(weight:Long,cost:V=%3ELong,maxBufferingDuration:org.joda.time.Duration):com.spotify.scio.values.SCollection[(K,Iterable[V])]).

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection

val elements: SCollection[String] = ???
val batchedElements: SCollection[Iterable[String]] = elements.batch(10)
```

## Misc

Some elements of an `SCollection` can be randomly sampled using @scaladoc[sample](com.spotify.scio.values.SCollection#sample(withReplacement:Boolean,fraction:Double):com.spotify.scio.values.SCollection[T]):

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val elements: SCollection[String] = ???
val result: SCollection[String] = elements.sample(withReplacement = true, fraction = 0.01)
```

The `SCollection` can be randomly split into new `SCollections` given a weighting of what fraction of the input should be in each split:

```scala
import com.spotify.scio.values.SCollection

val elements: SCollection[Int] = ???
val weights: Array[Double] = Array(0.2, 0.6, 0.2)
val splits: Array[SCollection[Int]] = elements.randomSplit(weights)
```

The "top" _n_ elements of an `SCollection` given some @scaladoc[Ordering](scala.math.Ordering) can be found with @scaladoc[top](com.spotify.scio.values.SCollection#top(num:Int)(implicitord:Ordering[T]):com.spotify.scio.values.SCollection[Iterable[T]]):

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val elements: SCollection[Int] = ???
val top10: SCollection[Iterable[Int]] = elements.top(10)
```

The common elements of two `SCollections` can be found with @scaladoc[intersection](com.spotify.scio.values.SCollection#intersection(that:com.spotify.scio.values.SCollection[T]):com.spotify.scio.values.SCollection[T]):

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val a: SCollection[String] = ???
val b: SCollection[String] = ???
val common: SCollection[String] = a.intersection(b)
```

For a keyed `SCollection`, @scaladoc[intersectByKey](com.spotify.scio.values.PairSCollectionFunctions#intersectByKey(rhs:com.spotify.scio.values.SCollection[K]):com.spotify.scio.values.SCollection[(K,V)]) will give the elements in the LHS whose keys are in the RHS:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val a: SCollection[(String, Int)] = ???
val b: SCollection[String] = ???
val common: SCollection[(String, Int)] = a.intersectByKey(b)
```

Similarly, @scaladoc[subtract](com.spotify.scio.values.SCollection#subtract(that:com.spotify.scio.values.SCollection[T]):com.spotify.scio.values.SCollection[T]) (or @scaladoc[subtractByKey](com.spotify.scio.values.PairSCollectionFunctions#subtractByKey(rhs:com.spotify.scio.values.SCollection[K]):com.spotify.scio.values.SCollection[(K,V)])) will give the elements in the LHS that are not present in the RHS:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val a: SCollection[String] = ???
val b: SCollection[String] = ???
val notInB: SCollection[String] = a.subtract(b)
```
