dataflow-scala
==============

Scala API for Google Cloud Dataflow

![Dataslow](http://i.imgur.com/IRFWIXE.jpg)

# Getting Started

First install the [Google Cloud SDK](https://cloud.google.com/sdk/) and create a Cloud Storage bucket for your project.

Then clone this repository and publish artifacts locally.

```bash
git clone git@github.com:spotify/dataflow-scala.git
cd dataflow-scala
sbt publishLocal
```

# Running the Examples

You can execute the examples locally from SBT. By default pipelines will be executed using the `DirectPipelineRunner` and local filesystem will be used for input and output.

```
neville@localhost dataflow-scala $ sbt
[info] Loading global plugins from /home/neville/.sbt/0.13/plugins
[info] Loading project definition from /home/neville/src/gcp/dataflow-scala/project
[info] Set current project to root (in build file:/home/neville/src/gcp/dataflow-scala/)
> project dataflow-scala-examples
[info] Set current project to dataflow-scala-examples (in build file:/home/neville/src/gcp/dataflow-scala/)
> runMain com.spotify.cloud.dataflow.examples.WordCount \
--input=<INPUT FILE PATTERN> \
--output=<OUTPUT DIRECTORY>
```

You can use the `BlockingDataflowPipelineRunner` or `DataflowPipelineRunner` to execute pipelines on Google Cloud Dataflow Service using managed resources in the Google Cloud Platform.

```
neville@localhost dataflow-scala $ sbt
[info] Loading global plugins from /home/neville/.sbt/0.13/plugins
[info] Loading project definition from /home/neville/src/gcp/dataflow-scala/project
[info] Set current project to root (in build file:/home/neville/src/gcp/dataflow-scala/)
> project dataflow-scala-examples
[info] Set current project to dataflow-scala-examples (in build file:/home/neville/src/gcp/dataflow-scala/)
> runMain com.spotify.cloud.dataflow.examples.WordCount \
--project=<YOUR CLOUD PLATFORM PROJECT NAME> \
--stagingLocation=<YOUR CLOUD STORAGE LOCATION> \
--runner=BlockingDataflowPipelineRunner \
--input=<INPUT FILE PATTERN> \
--output=<OUTPUT DIRECTORY>
```

Your Cloud Storage location should be entered in the form of `gs://bucket/path/to/staging/directory`. The Cloud Platform project refers to its name (not number).

# dataflow-scala vs. Spark

The dataflow-scala API is modeled after Spark with some minor differences.

- `SCollection` is equivalent to Spark's [`RDD`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD).
- `PairSCollectionFunctions` and `DoubleSCollectionFunctions` are specialized versions of `SCollection` and equivalent to Spark's [`PairRDDFunctions`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions) and [`DoubleRDDFunctions`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.DoubleRDDFunctions).
- Execution planning is static and happens before the job is submitted. This means that one can only perform the equivalent of Spark _transformations_ (`RDD` &rarr; `RDD`) but not _actions_ which return a value to the driver.
- There is no _broadcast_ either but the pattern of `RDD` to value in driver via _action_ and back to `RDD` via _broadcast_ can be replaced with a _transformation_ to `SCollection` of a single value and back to `SCollection` via _side input_.
- There is no `DStream` (continuous series of `RDD`s) like in Spark Streaming. Values in a `SCollection` are windowed based on timestamp and windowing operation and the same API works regardless of batch (single global window) or streaming mode. Aggregation type _transformations_ that produce `SCollection`s of a single value in batch mode will produce one value each window in batch mode.
- `SCollection` has extra methods for side input, side output, and windowing.
- `SCollection` can be converted to `SCollectionWithAccumulator` (allowing custom counters), `SCollectionWithSideInput` (replicating a small `SCollection` to all left-hand side values), or `WindowedSCollection` (allowing access to window information).