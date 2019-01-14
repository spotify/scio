# Scio, Beam and Dataflow

Check out the [Beam Programming Guide](https://beam.apache.org/documentation/programming-guide/) first for a detailed explanation of the Beam programming model and concepts. Also see this comparison between [[Scio, Scalding and Spark]] APIs.

Scio aims to be a thin wrapper on top of Beam while offering idiomatic Scala style API.

## Basics

- [`ScioContext`](https://spotify.github.io/scio/api/com/spotify/scio/ScioContext.html) wraps [`Pipeline`](https://beam.apache.org/documentation/sdks/javadoc/2.0.0/org/apache/beam/sdk/Pipeline.html)
- [`SCollection`](https://spotify.github.io/scio/api/com/spotify/scio/values/SCollection.html) wraps [`PCollection`](https://beam.apache.org/documentation/sdks/javadoc/2.0.0/org/apache/beam/sdk/values/PCollection.html)
- [`ScioResult`](https://spotify.github.io/scio/api/com/spotify/scio/ScioResult.html) wraps [`PipelineResult`](https://beam.apache.org/documentation/sdks/javadoc/2.0.0/org/apache/beam/sdk/PipelineResult.html)
- Most [`PTransform`](https://beam.apache.org/documentation/sdks/javadoc/2.0.0/org/apache/beam/sdk/transforms/PTransform.html) are implemented as idiomatic Scala methods on `SCollection` e.g. `map`, `flatMap`, `filter`, `reduce`.
- [`PairSCollectionFunctions`](https://spotify.github.io/scio/api/com/spotify/scio/values/PairSCollectionFunctions.html) and [`DoubleSCollectionFunctions`](https://spotify.github.io/scio/api/com/spotify/scio/values/DoubleSCollectionFunctions.html) are specialized version of `SCollection` implemented via the Scala "[pimp my library](https://coderwall.com/p/k_1jzw/scala-s-pimp-my-library-pattern-example)" pattern.
- An `SCollection[(K, V)]` is automatically converted to a `PairSCollectionFunctions` which provides key-value operations, e.g. `groupByKey`, `reduceByKey`, `cogroup`, `join`.
- An `SCollection[Double]` is automatically converted to a `DoubleSCollectionFunctions` which provides statistical operations, e.g. `stddev`, `variance`.

## ScioContext, PipelineOptions, Args and ScioResult

- Beam/Dataflow uses [`PipelineOptions`](https://beam.apache.org/documentation/sdks/javadoc/2.0.0/org/apache/beam/sdk/options/PipelineOptions.html) and its subclasses to parse command line arguments. Users have to extend the interface for their application level arguments.
- Scalding uses [`Args`](https://twitter.github.io/scalding/api/#com.twitter.scalding.Args) to parse application arguments in a more generic and boilerplate free style.
- `ScioContext` has a `parseArguments` method that takes an `Array[String]` of command line arguments, parses Beam/Dataflow specific ones into a `PipelineOptions`, and application specific ones into an `Args`, and returns the `(PipelineOptions, Args)`.
- `ContextAndArgs` is a short cut to create a `(ScioContext, Args)`.
- `ScioResult` can be used to access accumulator values and job state.

## IO

- Most [`IO`](https://beam.apache.org/documentation/sdks/javadoc/2.0.0/org/apache/beam/sdk/io/package-summary.html) Read transforms are implemented as methods on `ScioContext`, e.g. `avroFile`, `textFile`, `bigQueryTable`.
- Most `IO` Write transforms are implemented as methods on `SCollection`, e.g. `saveAsAvroFile`, `saveAsTextFile`, `saveAsBigQueryTable`.
- These IO operations also detects when the `ScioContext` is running in a [`JobTest`](https://spotify.github.io/scio/api/com/spotify/scio/testing/JobTest$.html) and manages test IO in memory.
- Write options also return a `Future[Tap[T]]` where [`Tap`](https://spotify.github.io/scio/api/com/spotify/scio/io/Tap.html) abstract away the logic of reading the dataset directly as an `Iterator[T]` or re-opening it in another `ScioContext`. The `Future` is complete once the job finishes. This can be used to do light weight pipeline orchestration e.g. [WordCountOrchestration.scala](../tree/master/scio-examples/src/main/scala/com/spotify/scio/examples/extra/WordCountOrchestration.scala).

## ByKey operations

- Beam/Dataflow `ByKey` transforms require `PCollection[KV[K, V]]` inputs while Scio uses `SCollection[(K, V)]`
- Hence every `ByKey` transform in `PairSCollectionFunctions` converts Scala `(K, V)` to `KV[K, V]` before and vice versa afterwards. However these are lightweight wrappers and the JVM should be able to optimize them.
- `PairSCollectionFunctions` also converts `java.lang.Iterable[V]` and `java.util.List[V]` to `scala.Iterable[V]` in some cases.

## Coders

- Beam/Dataflow uses [`Coder`](https://beam.apache.org/documentation/sdks/javadoc/2.0.0/org/apache/beam/sdk/coders/Coder.html) for (de)serializing elements in a `PCollection` during shuffle. There are built-in coders for Java primitive types, collections, and common types in GCP like Avro, ProtoBuf, BigQuery `TableRow`, Datastore `Entity`.
- `PCollection` uses [`TypeToken`](https://google.github.io/guava/releases/snapshot/api/docs/com/google/common/reflect/TypeToken.html) from [Guava reflection](https://github.com/google/guava/wiki/ReflectionExplained) and [`TypeDescriptor`](https://beam.apache.org/documentation/sdks/javadoc/2.0.0/org/apache/beam/sdk/values/TypeDescriptor.html) to workaround Java type erasure and retrieve type information of elements. This may not always work but there is a `PCollection#setCoder` method to override.
- Twitter's [chill](https://github.com/twitter/chill) library uses [kryo](https://github.com/EsotericSoftware/kryo) to (de)serialize data. Chill includes serializers for common Scala types and cal also automatically derive serializers for arbitrary objects. Scio fallbacks to [`KryoAtomicCoder`](https://github.com/spotify/scio/blob/master/scio-core/src/main/scala/com/spotify/scio/coders/KryoAtomicCoder.scala) when a built-in one isn't available.
- A coder may be non-deterministic if `Coder#verifyDeterministic` throws an exception. Any data type with such a coder cannot be used as a key in `ByKey` operations. However `KryoAtomicCoder` assumes all types are deterministic for simplicity so it's up to the user's discretion to not avoid non-deterministic types e.g. tuples or case classes with doubles as keys.
- Avro [`GenericRecord`](https://avro.apache.org/docs/current/api/java/org/apache/avro/generic/GenericRecord.html) requires a schema during deserialization (which is available as `GenericRecord#getSchema` for serialization) and [`AvroCoder`](https://beam.apache.org/documentation/sdks/javadoc/2.0.0/org/apache/beam/sdk/coders/AvroCoder.html) requires that too during initialization. This is not possible in `KryoAtomicCoder`, i.e. when nesting `GenericRecord` inside a Scala type. Instead `KryoAtomicCoder` serializes the schema before every record so that they can roundtrip safely. This is not optimal but the only way without requiring user to handcraft a custom coder.

