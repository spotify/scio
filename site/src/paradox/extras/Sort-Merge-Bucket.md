# Sort Merge Bucket

Sort Merge Bucket is a technique for writing data to file system in deterministic file locations,
sorted according by some pre-determined key, so that it can later be read in as key groups with
no shuffle required. Since each element is assigned a file destination (bucket) based on a hash
of its join key, we can use the same technique to cogroup multiple Sources as long as they're
written using the same key and hashing scheme.

For example, given these input records, and SMB write will first extract the key, assign the record
to a bucket, sort values within the bucket, and write these values to a corresponding file.

|        Input        |   Key   |   Bucket   |       File Assignment      |
|-------------------------------------------------------------------------|
| {key:"b", value: 1} |   "b"   |      0     | bucket-00000-of-00001.avro |
| {key:"b", value: 2} |   "b"   |      0     | bucket-00000-of-00001.avro |
| {key:"a", value: 3} |   "a"   |      1     | bucket-00001-of-00001.avro |

Two sources can be joined by opening file readers on corresponding buckets of eachT source and
merging key-groups as we go.

## What are SMB transforms?

`scio-smb` provides three @javadoc[PTransform](org.apache.beam.sdk.transforms.PTransform)s,
as well as corresponding Scala API bindings, for SMB operations:

- @javadoc[SortedBucketSink](org.apache.beam.sdk.extensions.smb.SortedBucketSink) writes data
to file system in SMB format.
    Scala APIs (see: @scaladoc[SortedBucketSCollection](com.spotify.scio.smb.syntax.SortedBucketSCollection)):

       * `SCollection[T: Coder]#saveAsSortedBucket`
    
    @@snip [SortMergeBucketExample.scala](/scio-examples/src/main/scala/com/spotify/scio/examples/extra/SortMergeBucketExample.scala) { #SortMergeBucketExample_sink }

    Note the use of `Integer` for parameterized key type instead of a Scala `Int`. The key class
    must have a Coder available in the default Beam (Java) coder registry.
    
    Also note that the number of buckets specified must be a power of 2. This allows sources of different
    bucket sizes to still be joinable.

- @javadoc[SortedBucketSource](org.apache.beam.sdk.extensions.smb.SortedBucketSource) reads
data that has been written to file system using `SortedBucketSink` into a collection of
@javadoc[CoGbkResult](org.apache.beam.sdk.transforms.join.CoGbkResult)s.
    Scala APIs (see: @scaladoc[SortedBucketScioContext](com.spotify.scio.smb.syntax.SortedBucketScioContext)):
      
      * `ScioContext#sortMergeGroupByKey` (1 source)
      * `ScioContext#sortMergeJoin` (2 sources)
      * `ScioContext#sortMergeCoGroup` (1-4 sources)
 
    @@snip [SortMergeBucketExample.scala](/scio-examples/src/main/scala/com/spotify/scio/examples/extra/SortMergeBucketExample.scala) { #SortMergeBucketExample_join }

- @javadoc[SortedBucketTransform](org.apache.beam.sdk.extensions.smb.SortedBucketTransform) reads
data that has been written to file system using `SortedBucketSink`, transforms each
@javadoc[CoGbkResult](org.apache.beam.sdk.transforms.join.CoGbkResult) using a user-supplied
function, and immediately rewrites them using the same bucketing scheme.
    Scala APIs (see: @scaladoc[SortedBucketScioContext](com.spotify.scio.smb.syntax.SortedBucketScioContext)):

      * `ScioContext#sortMergeTransform` (1-3 sources)
            
    @@snip [SortMergeBucketExample.scala](/scio-examples/src/main/scala/com/spotify/scio/examples/extra/SortMergeBucketExample.scala) { #SortMergeBucketExample_transform }

## What kind of data can I write using SMB?

SMB writes are supported for Avro (GenericRecord and SpecificRecord), JSON, and Tensorflow records.
See API bindings in:

- @javadoc[AvroSortedBucketIO](org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO)
- @javadoc[JsonSortedBucketIO](org.apache.beam.sdk.extensions.smb.JsonSortedBucketIO)
- @javadoc[TensorFlowBucketIO](org.apache.beam.sdk.extensions.smb.TensorFlowBucketIO)

## Tuning parameters for SMB transforms

SMB reads should be more performant and less resource-intensive than regular joins or groupBys.
However, SMB writes are more expensive than their regular counterparts, since they involve an extra
group-by (bucketing) and sorting step.

Additionally, non-SMB writes (i.e. implementations of
@javadoc[FileBasedSink](org.apache.beam.sdk.io.FileBasedSink)) use hints from the runner to determine
an optimal number of output files. With SMB, you must specify the number of buckets and shards
(`numBuckets` * `numShards` = total # of files) up front.
 
- A good starting point is to look at your output data as it has been written by a non-SMB sink,
  and pick the closest power of 2 as your initial `numBuckets`, and set `numShards` to 1.
- If you anticipate having hot keys, try increasing `numShards` to randomly split data within a bucket.
- If your job gets stuck in the sorting phase (since the `GroupByKey` and `SortValues` transforms
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
Alternately, `TargetParallelism.of(Integer value)` can be used to statically configure a custom value.

If no value is specified, SMB read operations will use the minimum parallelism.

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

## Testing
Currently, mocking data for SMB transforms is not supported in the `com.spotify.scio.testing.JobTest` framework. See
@github[SortMergeBucketExampleTest](/scio-examples/src/test/scala/com/spotify/scio/examples/extra/SortMergeBucketExampleTest.scala)
for an example of using local temp directories to test SMB reads and writes.
