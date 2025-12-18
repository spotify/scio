# Sort Merge Bucket

Sort Merge Bucket (SMB) is a technique for writing data to file system in deterministic file locations, sorted according to some pre-determined key, so that it can later be read in as key groups with no shuffle required.
Since each element is assigned a file destination (bucket) based on a hash of its join key, we can use the same technique to cogroup multiple Sources as long as they're written using the same key and hashing scheme.

For example, given these input records, and SMB write will first extract the key, assign the record to a bucket, sort values within the bucket, and write these values to a corresponding file.

|        Input        |   Key   |   Bucket   |       File Assignment      |
|-------------------------------------------------------------------------|
| {key:"b", value: 1} |   "b"   |      0     | bucket-00000-of-00002.avro |
| {key:"b", value: 2} |   "b"   |      0     | bucket-00000-of-00002.avro |
| {key:"a", value: 3} |   "a"   |      1     | bucket-00001-of-00002.avro |

Two sources can be joined by opening file readers on corresponding buckets of each source and merging key-groups as we go.

## What are SMB transforms?

`scio-smb` provides three @javadoc[PTransform](org.apache.beam.sdk.transforms.PTransform)s, as well as corresponding Scala API bindings, for SMB operations:

- @javadoc[SortedBucketSink](org.apache.beam.sdk.extensions.smb.SortedBucketSink) writes data to file system in SMB format.
    Scala APIs (see: @scaladoc[SortedBucketSCollection](com.spotify.scio.smb.syntax.SortedBucketSCollection)):

       * `SCollection[T: Coder]#saveAsSortedBucket`
    
    @@snip [SortMergeBucketExample.scala](/scio-examples/src/main/scala/com/spotify/scio/examples/extra/SortMergeBucketExample.scala) { #SortMergeBucketExample_sink }

    Note the use of `Integer` for parameterized key type instead of a Scala `Int`.
    The key class must have a Coder available in the default Beam (Java) coder registry.
    
    Also note that the number of buckets specified must be a power of 2.
    This allows sources of different bucket sizes to still be joinable.

- @javadoc[SortedBucketSource](org.apache.beam.sdk.extensions.smb.SortedBucketSource) reads data that has been written to file system using `SortedBucketSink` into a collection of
@javadoc[CoGbkResult](org.apache.beam.sdk.transforms.join.CoGbkResult)s.
    Scala APIs (see: @scaladoc[SortedBucketScioContext](com.spotify.scio.smb.syntax.SortedBucketScioContext)):
      
      * `ScioContext#sortMergeGroupByKey` (1 source)
      * `ScioContext#sortMergeJoin` (2 sources)
      * `ScioContext#sortMergeCoGroup` (1-22 sources)
 
    Note that each @javadoc[TupleTag](org.apache.beam.sdk.values.TupleTag) used to create the @javadoc[SortedBucketIO.Read](org.apache.beam.sdk.extensions.smb.SortedBucketIO.Read)s needs to have a unique Id.

    @@snip [SortMergeBucketExample.scala](/scio-examples/src/main/scala/com/spotify/scio/examples/extra/SortMergeBucketExample.scala) { #SortMergeBucketExample_join }

- @javadoc[SortedBucketTransform](org.apache.beam.sdk.extensions.smb.SortedBucketTransform) reads data that has been written to file system using `SortedBucketSink`, transforms each @javadoc[CoGbkResult](org.apache.beam.sdk.transforms.join.CoGbkResult) using a user-supplied function, and immediately rewrites them using the same bucketing scheme.
    Scala APIs (see: @scaladoc[SortedBucketScioContext](com.spotify.scio.smb.syntax.SortedBucketScioContext)):

      * `ScioContext#sortMergeTransform` (1-22 sources)

    Note that each @javadoc[TupleTag](org.apache.beam.sdk.values.TupleTag) used to create the @javadoc[SortedBucketIO.Read](org.apache.beam.sdk.extensions.smb.SortedBucketIO.Read)s needs to have a unique Id.

    @@snip [SortMergeBucketExample.scala](/scio-examples/src/main/scala/com/spotify/scio/examples/extra/SortMergeBucketExample.scala) { #SortMergeBucketExample_transform }

## SMBCollection: Fluent API for SMB operations

_Since Scio 0.15.0_ (experimental).

@scaladoc[SMBCollection](com.spotify.scio.smb.SMBCollection) provides a fluent, `SCollection`-like API for SMB operations with built-in support for **zero-shuffle multi-output** transforms.

### Key Features

- **Fluent transformations**: Chain operations like `map`, `filter`, `flatMap` while preserving SMB structure
- **Zero-shuffle multi-output**: Multiple outputs from the same base read SMB data once with shared computation
- **Auto-execution**: Outputs execute automatically via `sc.onClose()` hook (no explicit `.run()` needed)
- **Type-safe**: Full Scala type safety with coders

### Why Use Multi-Output?

Multi-output SMB transforms solve a common problem: creating multiple derived datasets from the same expensive cogroup or computation. Traditional approaches have significant overhead:

**Problem: Traditional Approach (N reads + N shuffles)**
```scala
// Approach 1: Separate transforms - reads data 3 times, 3 shuffles
sc.sortMergeTransform(users, accounts).to(summaryOutput).via(expensiveCompute)
sc.sortMergeTransform(users, accounts).to(detailsOutput).via(expensiveCompute)
sc.sortMergeTransform(users, accounts).to(highValueOutput).via(expensiveCompute)

// Approach 2: SCollection fanout - 1 read, but 3 shuffles (GroupByKey in each saveAsSortedBucket)
val result = sc.sortMergeJoin(users, accounts).map(expensiveCompute)
result.map(_.summary).saveAsSortedBucket(summaryOutput)
result.map(_.details).saveAsSortedBucket(detailsOutput)
result.filter(_.isHighValue).saveAsSortedBucket(highValueOutput)
```

**Solution: SMBCollection Multi-Output (1 read + 0 shuffles)**
```scala
val base = SMBCollection.cogroup2(classOf[String], usersRead, accountsRead)
  .map { case (_, (users, accounts)) => expensiveCompute(users, accounts) }  // Runs ONCE per key group

// Fan out to multiple outputs - all data already bucketed!
base.map(_.summary).saveAsSortedBucket(summaryOutput)
base.map(_.details).saveAsSortedBucket(detailsOutput)
base.filter(_.isHighValue).saveAsSortedBucket(highValueOutput)

sc.run()  // Executes in single pass with shared I/O
```

### Complete Example: Multi-Output Transform

This example demonstrates creating three different outputs from a single expensive join:

@@snip [SortMergeBucketExample.scala](/scio-examples/src/main/scala/com/spotify/scio/examples/extra/SortMergeBucketExample.scala) { #SortMergeBucketExample_multi_output }

**Performance characteristics:**
- ✅ SMB data read **once** (not 3 times)
- ✅ Expensive cogroup computation runs **once** per key group (not 3 times)
- ✅ **Zero shuffles** - data stays bucketed through entire pipeline
- ✅ Automatically fuses transformations at runtime

**When to use this pattern:**
- Multiple downstream datasets derived from the same join
- Expensive aggregations or computations that feed multiple outputs
- Reducing pipeline cost and latency by eliminating redundant work

**Cost savings examples:**

| Scenario | Traditional (SCollection fanout) | Fluent Multi-Output | Cost Reduction |
|----------|----------------------------------|---------------------|----------------|
| 1TB → 3 SMB outputs | 1TB read + ~3TB shuffle | 1TB read, 0 shuffle | **~4× savings** |
| 2TB join → 5 outputs | 2TB read + ~10TB shuffle | 2TB read, 0 shuffle | **~6× savings** |
| 500GB → 10 outputs | 500GB read + ~5TB shuffle | 500GB read, 0 shuffle | **~11× savings** |

The savings scale with the number of outputs - each additional output adds another shuffle in traditional approach, but costs nothing with fluent multi-output.

### Transforming Values

Transformations work directly on values:

```scala
val result = SMBCollection.read(classOf[String], usersRead)
  .filter(_.isActive)
  .map(_.email)
  .saveAsSortedBucket(output)
```

### Converting to SCollection

SMBCollection can convert to regular `SCollection` for operations that require it:

```scala
// Option 1: Deferred execution (allows multi-output)
val deferred = SMBCollection.read(classOf[Integer], usersRead)
  .map(transform)
  .toDeferredSCollection()

val sc = deferred.get  // Materialize when ready

// Option 2: Immediate conversion
val sc = SMBCollection.read(classOf[Integer], usersRead)
  .toSCollectionAndSeal()  // Equivalent to .toDeferredSCollection().get
```

You can even **mix SMB and SCollection outputs** from the same base:

```scala
val base = SMBCollection.cogroup2(classOf[Integer], usersRead, accountsRead)
  .map { case (_, (users, accounts)) => expensiveJoin(users, accounts) }  // Runs ONCE

// SMB outputs (stay bucketed)
base.map(_.summary).saveAsSortedBucket(summaryOutput)
base.map(_.details).saveAsSortedBucket(detailsOutput)

// SCollection output (for non-SMB operations)
val sc = base.toDeferredSCollection().get
sc.filter(_.needsProcessing).saveAsTextFile(textOutput)

sc.run()  // All outputs execute in one pass!
```

### Side Inputs

SMBCollection supports side inputs via `.withSideInputs()`:

```scala
val sideInput = sc.parallelize(List("config")).asSingletonSideInput

SMBCollection.read(classOf[Integer], usersRead)
  .withSideInputs(sideInput)
  .map { (ctx, values) =>
    val config = ctx(sideInput)
    values.map(v => transform(v, config))
  }
  .saveAsSortedBucket(output)
```

### Limitations

- **Cogroup arity**: Currently supports up to 4-way cogroups (`cogroup2`, `cogroup3`, `cogroup4`). For 5-22 way cogroups, use traditional `sortMergeCoGroup` API. Note: this is not a systemic limitation - the API can be easily extended to support higher arity by adding `cogroup5` through `cogroup22` methods.
- **Key preservation required**: Transformations must not change the embedded key value
- **Experimental API**: Subject to changes in future Scio versions

### When to Use Fluent vs Traditional API

**Use SMBCollection (Fluent API)** - Default choice for most SMB operations:
- ✅ Any SMB → SMB transformation (cleaner functional syntax vs imperative callbacks)
- ✅ Multiple SMB outputs from same computation (**massive cost savings** via zero-shuffle)
- ✅ Mixed SMB + SCollection outputs (via `toDeferredSCollection()`)
- ✅ 2-4 source cogroups
- ✅ Clean functional operations (`map`, `filter`, `flatMap`) directly on values

**Use Traditional API** - Only when required:
- ✅ 5-22 way cogroups (fluent API limitation - though easily extensible)

**Note**: For SMB → SCollection conversions, both APIs are effectively equivalent. Fluent uses `.toSCollectionAndSeal()` vs traditional's direct `SCollection` return - the difference is trivial.

The fluent API uses familiar functional operations (`map`, `flatMap`, `filter`) instead of traditional API's imperative `outputCollector.accept()` callbacks, making it more idiomatic for Scala developers.

See @scaladoc[SMBCollection](com.spotify.scio.smb.SMBCollection) for full API documentation.

### API Comparison

Quick reference for choosing between fluent and traditional APIs:

| Feature | Fluent API | Traditional API | Notes |
|---------|------------|-----------------|-------|
| 2-4 source cogroups | ✅ | ✅ | Both work equally well |
| 5-22 source cogroups | ❌ | ✅ | Traditional only (fluent limitation - easily extensible) |
| SMB → multiple SMB outputs | ✅ Zero shuffle | ⚠️ Requires shuffles | **Fluent major advantage** |
| SMB → mixed SMB + SCollection | ✅ | ❌ | Fluent only |
| Functional syntax | ✅ `map`/`filter`/`flatMap` | ❌ `outputCollector.accept()` | Fluent more idiomatic |
| SMB → SCollection | ✅ `.toSCollectionAndSeal()` | ✅ Direct return | Effectively equivalent |

## What kind of data can I write using SMB?

SMB writes are supported for multiple formats:

- Avro (GenericRecord and SpecificRecord) when also depending on `scio-avro`.
    - @javadoc[AvroSortedBucketIO](org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO)
- JSON
    - @javadoc[JsonSortedBucketIO](org.apache.beam.sdk.extensions.smb.JsonSortedBucketIO)
- Parquet when also depending on  `scio-parquet`
    - @javadoc[ParquetAvroSortedBucketIO](org.apache.beam.sdk.extensions.smb.ParquetAvroSortedBucketIO)
    - @javadoc[ParquetTypesSortedBucketIO](org.apache.beam.sdk.extensions.smb.ParquetTypesSortedBucketIO)
- Tensorflow when also depending on `scio-tensorflow`
    - @javadoc[TensorFlowBucketIO](org.apache.beam.sdk.extensions.smb.TensorFlowBucketIO)

## Secondary keys

_Since Scio 0.12.0_.

A single key group may be very large and the implementation of SMB requires either handling the elements of the key group iteratively or loading the entire key group into memory.
In the case where a secondary grouping or sorting is required, this can be prohibitive in terms of memory and/or wasteful when multiple downstream pipelines do the same grouping.
For example, a SMB dataset might be keyed by `user_id` but many downstreams want to group by the tuple of `(user_id, artist_id)`.

Secondary SMB keys enable this use-case by sorting pipeline output by the hashed primary SMB key as described above, then additionally sorting the output for each key by the secondary SMB key.
When key groups are read by a downstream pipeline it may read either the entire (primary) key group or the subset of elements belonging to the (primary key, secondary key) tuple.

_A dataset may therefore add a secondary key and remain compatible with any downstream readers which expect only a primary key._

To write with a secondary key, the additional key class and path must be provided:

@@snip [SortMergeBucketExample.scala](/scio-examples/src/main/scala/com/spotify/scio/examples/extra/SortMergeBucketExample.scala) { #SortMergeBucketExample_sink_secondary }

To read with a secondary key, the additional key class must be provided:

@@snip [SortMergeBucketExample.scala](/scio-examples/src/main/scala/com/spotify/scio/examples/extra/SortMergeBucketExample.scala) { #SortMergeBucketExample_secondary_read }

Corresponding secondary-key-enabled variants of `sortMergeJoin`, `sortMergeCogroup`, and `sortMergeTransform` are also included.


## Null keys

If the key field of one or more PCollection elements is null, those elements will be diverted into a special
bucket file, `bucket-null-keys.avro`. This file will be ignored in SMB reads and transforms and must
be manually read by a downstream user.

## Avro String keys

As of **Scio 0.14.0**, Avro `CharSequence` are backed by `String` instead of default `Utf8`.
With previous versions you may encounter the following when using Avro `CharSequence` keys:

```bash
Cause: java.lang.ClassCastException: class org.apache.avro.util.Utf8 cannot be cast to class java.lang.String
[info]   at org.apache.beam.sdk.coders.StringUtf8Coder.encode(StringUtf8Coder.java:37)
[info]   at org.apache.beam.sdk.extensions.smb.BucketMetadata.encodeKeyBytes(BucketMetadata.java:222)
```

You'll have to either recompile your avro schema using `String` type,
or add the `GenericData.StringType.String` property to your Avro schema with [setStringType](https://avro.apache.org/docs/1.11.1/api/java/org/apache/avro/generic/GenericData.html#setStringType-org.apache.avro.Schema-org.apache.avro.generic.GenericData.StringType-)

## Parquet

SMB supports Parquet reads and writes in both Avro and case class formats.

As of **Scio 0.14.0** and above, Scio supports specific record logical types in parquet-avro out of the box.

When using generic record, you have to manually supply a _data supplier_ in your Parquet `Configuration` parameter.
See @ref:[Logical Types in Parquet](../io/Parquet.md#logical-types) for more information.

## Tuning parameters for SMB transforms

### numBuckets/numShards
SMB reads should be more performant and less resource-intensive than regular joins or groupBys.
However, SMB writes are more expensive than their regular counterparts, since they involve an extra
group-by (bucketing) and sorting step.  Additionally, non-SMB writes (i.e. implementations of
@javadoc[FileBasedSink](org.apache.beam.sdk.io.FileBasedSink)) use hints from the runner to determine
an optimal number of output files. Unfortunately, SMB doesn't have access to those runtime hints;
you must specify the number of buckets and shards as static values up front.

In SMB, *buckets* correspond to the hashed value of the SMB key % a given power of 2. A record with a given key will _always_ be hashed
into the same bucket. On the file system, buckets consist of one or more *sharded files* in which
records are randomly assigned per-bundle. Two records with the same key may end up in different shard files within a bucket.
 
- A good starting point is to look at your output data as it has been written by a non-SMB sink,
  and pick the closest power of 2 as your initial `numBuckets` and set `numShards` to 1.
- If you anticipate having hot keys, try increasing `numShards` to randomly split data within a bucket.
- `numBuckets` * `numShards` = total # of files written to disk.

### sorterMemoryMb
If your job gets stuck in the sorting phase (since the `GroupByKey` and `SortValues` transforms
  may get fused--you can reference the @javadoc[Counter](org.apache.beam.sdk.metrics.Counter)s
  `SortedBucketSink-bucketsInitiatedSorting` and `SortedBucketSink-bucketsCompletedSorting`
  to get an idea of where your job fails), you can increase sorter memory
  (default is 1024MB, or 128MB for Scio <= 0.9.0):

```scala
data.saveAsSortedBucket(
  AvroSortedBucketIO
    .write[K, V](classOf[K], "keyField", classOf[V])
    .to(...)
    .withSorterMemoryMb(256)
)
```

The amount of data each external sorter instance needs to handle is `total output size / numBuckets
/ numShards`, and when this exceeds sorter memory, the sorter will spill to disk. `n1-standard`
workers has 3.75GB RAM per CPU, so 1GB sorter memory is a decent default, especially if the output
files are kept under that size. If you have to spill to disk, note that worker disk IO depends on
disk type, size, and worker number of CPUs.

See [specifying pipeline execution
parameters](https://cloud.google.com/dataflow/docs/guides/specifying-exec-params) for more details,
e.g. `--workerMachineType`, `--workerDiskType`, and `--diskSizeGb`. Also read more about [machine
types](https://cloud.google.com/compute/docs/machine-types) and [block storage
performance](https://cloud.google.com/compute/docs/disks/performance)

### Parallelism
The `SortedBucketSource` API accepts an optional
@javadoc[TargetParallelism](org.apache.beam.sdk.extensions.smb.TargetParallelism) parameter to set the
desired parallelism of the SMB read operation. For a given set of sources, `targetParallelism` can be
set to any number between the least and greatest numbers of buckets among sources. This can be
dynamically configured using `TargetParallelism.min()` or `TargetParallelism.max()`, which at graph
construction time will determine the least or greatest amount of parallelism based on sources.

Alternately, `TargetParallelism.of(Integer value)` can be used to statically configure a custom value,
or `{@link TargetParallelism#auto()}` can be used to let the runner decide how to split the SMB read
at runtime based on the combined byte size of the inputs--this is also the default behavior if
`TargetPallelism` is left unspecified.

If no value is specified, SMB read operations will use Auto parallelism.

When selecting a target parallelism for your SMB operation, there are tradeoffs to consider:

  - Minimal parallelism means a fewer number of workers merging data from potentially many
    buckets. For example, if source A has 4 buckets and source B has 64, a minimally parallel
    SMB read would have 4 workers, each one merging 1 bucket from source A and 16 buckets from
    source B. This read may have low throughput.
  - Maximal parallelism means that each bucket is read by at least one worker. For example, if
    source A has 4 buckets and source B has 64, a maximally parallel SMB read would have 64 workers,
    each one merging 1 bucket from source B and 1 bucket from source A, replicated 16 times. This
    may have better throughput than the minimal example, but more expensive because every key group
    from the replicated sources must be re-hashed to avoid emitting duplicate records.
  - A custom parallelism in the middle of these bounds may be the best balance of speed and
    computing cost.
  - Auto parallelism is more likely to pick an ideal value for most use cases. If its performance is
    worse than expected, you can look up the parallelism value it has computed and try a manual adjustment.
    Unfortunately, since it's determined at runtime, the computed parallelism value can't be added
    to the pipeline graph through `DisplayData`. Instead, you'll have to check the worker logs to find
    out which value was selected. When using Dataflow, you can do this in the UI by clicking on the
    SMB transform box, and searching the associated logs for the text `Parallelism was adjusted`.
    For example, in this case the value is 1024:

    <div style="text-align: center;"><img src="../images/smb_auto_parallelism_example.png" alt="Finding computed parallelism"
    style="margin: 10px auto; width: 75%" /></div>

    From there, you can try increasing or decreasing the parallelism by specifying a different
    `TargetParallelism` parameter to your SMB read. Often auto-parallelism will select a low value and
    using `TargetParallelism.max()` can help.

### Read buffering
Performance can suffer when reading an SMB source across many partitions if the total number of files
(`numBuckets` * `numShards` * `numPartitions`) is too large (on the order of hundreds of thousands to millions of files).
We've observed errors and timeouts as a result of too many simultaneous filesystem connections. To that end,
we've added two @javadoc[PipelineOptions](org.apache.beam.sdk.options.PipelineOptions) to Scio 0.10.3, settable either via command-line args
or using @javadoc[SortedBucketOptions](org.apache.beam.sdk.extensions.smb.SortedBucketOptions) directly.

  - `--sortedBucketReadBufferSize` (default: 10000): an Integer that determines the number of _elements_ to read and buffer
    from _each file_ at a time. For example, by default, each file will have 10,000 elements read and buffered into
    an in-memory array at worker startup. Then, the sort-merge algorithm will request them one at a time as needed. Once 10,000 elements
    have been requested, the file will buffer the next 10,000.

    *Note*: this can be quite memory-intensive and require bumping the worker memory. If you have a
    small number of files, or don't need this optimization, you can turn it off by setting `--sortedBucketReadBufferSize=0`.
  - `--sortedBucketReadDiskBufferMb` (default: unset): an Integer that, if set, will force each worker to
    actually copy the specified # of megabytes from the remote filesystem into the worker's local temp directory,
    rather than streaming directly from FS. This caching is done eagerly: each worker will read as much as it can of each file
    in the order they're requested, and more space will be freed up once a file is fully read. Note that this is
    a _per worker limit_.

## Testing

As of Scio 0.14, mocking data for SMB transforms is supported in the `com.spotify.scio.testing.JobTest` framework. Prior to Scio 0.14, you can test using real data written to local temp files.

### Testing SMB in JobTest

Scio 0.14 and above support testing SMB reads, writes, and transforms using @javadoc[SmbIO](com.spotify.scio.smb.SmbIO).

Consider the following sample job that contains an SMB read and write:

```scala mdoc
import org.apache.beam.sdk.extensions.smb.ParquetAvroSortedBucketIO
import org.apache.beam.sdk.values.TupleTag
import com.spotify.scio._
import com.spotify.scio.avro.Account
import com.spotify.scio.values.SCollection
import com.spotify.scio.smb._

object SmbJob {
    def main(cmdLineArgs: Array[String]): Unit = {
        val (sc, args) = ContextAndArgs(cmdLineArgs)
        
        // Read
        sc.sortMergeGroupByKey(
            classOf[Integer],
            ParquetAvroSortedBucketIO
                .read(new TupleTag[Account](), classOf[Account])
                .from(args("input"))
        )
        
        // Write
        val writeData: SCollection[Account] = ???
        writeData.saveAsSortedBucket(
            ParquetAvroSortedBucketIO
              .write(classOf[Integer], "id", classOf[Account])
              .to(args("output"))
        )
        
        sc.run().waitUntilDone()
    }
}
```

A JobTest can be wired in using `SmbIO` inputs and outputs. `SmbIO` is typed according to the record type and the SMB key type, and the SMB key function is required to construct it.

```scala mdoc
import com.spotify.scio.smb.SmbIO
import com.spotify.scio.testing.PipelineSpec

class SmbJobTest extends PipelineSpec {
    "SmbJob" should "work" in {
        val smbInput: Seq[Account] = ???
        
        JobTest[SmbJob.type]
              .args("--input=gs://input", "--output=gs://output")
             
              // Mock .sortMergeGroupByKey
              .input(SmbIO[Int, Account]("gs://input", _.getId), smbInput)
              
              // Mock .saveAsSortedBucket
              .output(SmbIO[Int, Account]("gs://output", _.getId)) { output =>
                // Assert on output
              }
              .run()
    }
}
```

SMB Transforms can be mocked by combining input and output `SmbIO`s:

```scala mdoc:compile-only
// Scio job
object SmbTransformJob {
    def main(cmdLineArgs: Array[String]): Unit = {
        val (sc, args) = ContextAndArgs(cmdLineArgs)
        sc.sortMergeTransform(
            classOf[Integer],
            ParquetAvroSortedBucketIO
                .read(new TupleTag[Account](), classOf[Account])
                .from(args("input"))
        ).to(
            ParquetAvroSortedBucketIO
                .transformOutput[Integer, Account](classOf[Integer], "id", classOf[Account])
                .to(args("output"))
        ).via { case (key, grouped, outputCollector) =>
          val output: Account = ???
          outputCollector.accept(output)
        }
        sc.run().waitUntilDone()
  }
}

// Job test
class SmbTransformJobTest extends PipelineSpec {
    "SmbTransformJob" should "work" in {
        val smbinput: Seq[Account] = ???
        
        JobTest[SmbTransformJob.type]
              .args("--input=gs://input", "--output=gs://output")
             
              // Mock SMB Transform input
              .input(SmbIO[Int, Account]("gs://input", _.getId), smbinput)
              
              // Mock SMB Transform output
              .output(SmbIO[Int, Account]("gs://output", _.getId)) { output =>
                // Assert on output
              }
              .run()
    }
}
```

See @github[SortMergeBucketExampleTest](/scio-examples/src/test/scala/com/spotify/scio/examples/extra/SortMergeBucketExampleTest.scala) for complete JobTest examples.

### Testing SMB using local file system

Using the JobTest framework for SMB reads, writes, and transforms is recommended, as it eliminates the need to manage local files and Taps. However, there are a few
cases where performing real reads and writes is advantageous:

- If you want to assert on SMB Predicates/Parquet FilterPredicates in reads, as these are skipped in JobTest
- If you want to assert on written metadata
- If you want to test schema evolution compatibility (i.e. writing using an updated record schema and reading using the original schema),
or on projected schema compatability (i.e. using a case class projection to read Parquet data written with an Avro schema)

Scio 0.14.0 and above automatically return Taps for SMB writes and transforms, and can materialize SMB reads into Taps:

```scala mdoc
import com.spotify.scio.io.ClosedTap

// Scio job
object SmbRealFilesJob {
    def write(sc: ScioContext, output: String): ClosedTap[Account] = {
        val writeData: SCollection[Account] = ???
        writeData.saveAsSortedBucket(
            ParquetAvroSortedBucketIO
              .write(classOf[Integer], "id", classOf[Account])
              .to(output)
        )
    }
    
    def read(sc: ScioContext, input: String): SCollection[(Integer, Iterable[Account])] = {
        sc.sortMergeGroupByKey(
            classOf[Integer],
            ParquetAvroSortedBucketIO
                .read(new TupleTag[Account](), classOf[Account])
                .from(input)
        )
    }
}

// Unit test
import java.nio.file.Files

class SmbLocalFilesTest extends PipelineSpec {
    "SmbRealFilesJob" should "write and read data" in {
        val dir = Files.createTempDirectory("smb").toString
        
        // Test write
        val (_, writtenData) = runWithOutput { sc =>
            SmbRealFilesJob.write(sc, dir)
        }

        // Assert on actual written output
        writtenData.value should have size 100
        
        // Test read in separate ScioContext
        val (_, groupedData) = runWithLocalOutput { sc =>
            SmbRealFilesJob.read(sc, dir)
        }

        // Assert on actual read result
        groupedData should have size 50
    }
}
```

In addition to JobTest examples, see @github[SortMergeBucketExampleTest](/scio-examples/src/test/scala/com/spotify/scio/examples/extra/SortMergeBucketExampleTest.scala) for complete SMB Tap examples.
