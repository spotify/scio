Scio
====

[![Build Status](https://travis-ci.org/spotify/scio.svg?branch=master)](https://travis-ci.org/spotify/scio)
[![codecov.io](https://codecov.io/github/spotify/scio/coverage.svg?branch=master)](https://codecov.io/github/spotify/scio?branch=master)
[![Maven Central](https://img.shields.io/maven-central/v/com.spotify/scio-core_2.11.svg)](https://maven-badges.herokuapp.com/maven-central/com.spotify/scio-core_2.11)

_Ecclesiastical Latin IPA: /ˈʃi.o/, [ˈʃiː.o], [ˈʃi.i̯o]_

_Verb: I can, know, understand, have knowledge._

Scio is a Scala API for [Google Cloud Dataflow](https://github.com/GoogleCloudPlatform/DataflowJavaSDK), inspired by [Spark](http://spark.apache.org/) and [Scalding](https://github.com/twitter/scalding). See the [current API documentation](http://spotify.github.io/scio/) for more information.

# Getting Started

First install the [Google Cloud SDK](https://cloud.google.com/sdk/) and create a [Google Cloud Storage](https://cloud.google.com/storage/) bucket for your project, e.g. `gs://my-bucket`. Make sure it's in the same region as the [BigQuery](https://cloud.google.com/bigquery/) datasets you want to access and where you want Dataflow to launch workers on GCE.

# Building Scio

Scio is built using [SBT](http://www.scala-sbt.org/). To build Scio and publish artifacts locally, run:

```bash
git clone git@github.com:spotify/scio.git
cd scio
sbt publish-local
```

# Running the Examples

You can execute the examples locally from SBT. By default pipelines will be executed using the [`DirectPipelineRunner`](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/runners/DirectPipelineRunner) and local filesystem will be used for input and output.

The `-Dbigquery.project=<BILLING_PROJECT>` argument is required to compile the typed BigQuery example since the underlying macro makes BigQuery requests at compile time.

```
neville@localhost scio $ sbt -Dbigquery.project=<BILLING_PROJECT>
[info] ...
> project scio-examples
[info] ...
> runMain com.spotify.scio.examples.WordCount
--input=<INPUT FILE PATTERN>
--output=<OUTPUT DIRECTORY>
```

Note that unlike Hadoop, Scio or Dataflow input should be file patterns and not directories, i.e. `gs://bucket/path/part-*.txt` and not `gs://bucket/path`. Output on the other hand should be directories just like Hadoop, so `gs://bucket/path` will produce files like `gs://bucket/path/part-00000-of-00005.txt`.

Use the [`BlockingDataflowPipelineRunner`](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/runners/BlockingDataflowPipelineRunner) or [`DataflowPipelineRunner`](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/runners/DataflowPipelineRunner) to execute pipelines on Google Cloud Dataflow Service using managed resources in the Google Cloud Platform. The former will block the main process on `ScioContext#close()` until job completes while the latter will submit the job and return immediately.

```
neville@localhost scio $ sbt -Dbigquery.project=<BILLING_PROJECT>
[info] ...
> project scio-examples
[info] ...
> runMain com.spotify.scio.examples.WordCount
--project=<YOUR CLOUD PLATFORM PROJECT NAME>
--stagingLocation=<YOUR CLOUD STORAGE LOCATION>
--zone=<GCE AVAILABILITY ZONE>
--runner=BlockingDataflowPipelineRunner
--input=<INPUT FILE PATTERN>
--output=<OUTPUT DIRECTORY>
```

Your Cloud Storage location should be entered in the form of `gs://bucket/path/to/staging/directory`. The Cloud Platform project refers to its name (not number).

GCE availability zone should be in the same region as the BigQuery datasets and GCS bucket.

# BigQuery Settings

You may need a few extra settings to use BigQuery queries as pipeline input.

```
sbt -Dbigquery.project=<PROJECT-NAME> -Dbigquery.staging_dataset.location=<LOCATION>
```

- `bigquery.project`: GCP project to make BigQuery requests with at compile time.
- `bigquery.staging_dataset.location`: Geographical location for BigQuery staging dataset, e.g. `US`, `EU`, must be the same as source tables and GCS buckets.

# Options

More Dataflow pipeline specific options available can be found in [`DataflowPipelineOptions`](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/options/DataflowPipelineOptions) and super interfaces. Some more useful ones are from [`DataflowPipelineWorkerPoolOptions`](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/options/DataflowPipelineWorkerPoolOptions):

- `--numWorkers`: Number of workers to use when executing the Dataflow job.
- `--autoscalingAlgorithm`: [Experimental] The autoscaling algorithm to use for the workerpool. `NONE`: does not change the size of the worker pool. `THROUGHPUT_BASED`: Autoscale the workerpool based on throughput (up to maxNumWorkers). (default=`NONE`)
- `--maxNumWorkers`: [Experimental] The maximum number of workers to use when using workerpool autoscaling. (default=20)
- `--diskSizeGb`: Remote worker disk size, in gigabytes, or 0 to use the default size.
- `--workerMachineType`: Machine type to create Dataflow worker VMs as. See [https://cloud.google.com/compute/docs/machine-types]() for a list of valid options. If unset, the Dataflow service will choose a reasonable default.
- `--network`: GCE network for launching workers.

# Scio, Spark and Scalding

The Scio API is heavily influenced by Spark but there are some minor differences.

- [`SCollection`](http://spotify.github.io/scio/#com.spotify.scio.values.SCollection) is equivalent to Spark's [`RDD`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD).
- [`PairSCollectionFunctions`](http://spotify.github.io/scio/#com.spotify.scio.values.PairSCollectionFunctions) and [`DoubleSCollectionFunctions`](http://spotify.github.io/scio/#com.spotify.scio.values.DoubleSCollectionFunctions) are specialized versions of `SCollection` and equivalent to Spark's [`PairRDDFunctions`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions) and [`DoubleRDDFunctions`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.DoubleRDDFunctions).
- Execution planning is static and happens before the job is submitted. There is no driver node in a Dataflow cluster and one can only perform the equivalent of Spark [_transformations_](http://spark.apache.org/docs/latest/programming-guide.html#transformations) (`RDD` &rarr; `RDD`) but not [_actions_](http://spark.apache.org/docs/latest/programming-guide.html#actions) (`RDD` &rarr; driver local memory).
- There is no [_broadcast_](http://spark.apache.org/docs/latest/programming-guide.html#broadcast-variables) either but the pattern of `RDD` &rarr; driver via _action_ and driver &rarr; `RDD` via _broadcast_ can be replaced with `SCollection.asSingleTonSideInput` and `SCollection.withSideInputs`.
- There is no [`DStream`](https://spark.apache.org/docs/latest/streaming-programming-guide.html#discretized-streams-dstreams) (continuous series of `RDD`s) like in Spark Streaming. Values in a `SCollection` are windowed based on timestamp and windowing operation. The same API works regardless of batch (single global window by default) or streaming mode. Aggregation type _transformations_ that produce `SCollection`s of a single value under global window will produce one value each window when a non-global window is defined.
- `SCollection` has extra methods for side input, side output, and windowing.

Some features may look familiar to Scalding users.

- [`Args`](http://spotify.github.io/scio/#com.spotify.scio.Args) is a simple command line argument parser similar to the one in Scalding.
- Powerful transforms are possible with `sum`, `sumByKey`, `aggregate`, `aggregrateByKey` using [Algebird](https://github.com/twitter/algebird) `Semigroup`s and `Aggregator`s.
- [`MultiJoin`](http://spotify.github.io/scio/#com.spotify.scio.util.MultiJoin$) and coGroup of up to 22 sources.
- [`JobTest`](http://spotify.github.io/scio/#com.spotify.scio.testing.JobTest$) for end to end pipeline testing.

`SCollection` has a few variations.

- [`SCollectionWithAccumulator`](http://spotify.github.io/scio/#com.spotify.scio.values.SCollectionWithAccumulator) for accessing custom counters similar to those in Hadoop. See [AccumulatorExample.scala](https://github.com/spotify/scio/blob/master/scio-examples/src/main/scala/com/spotify/scio/examples/extra/AccumulatorExample.scala).
- [`SCollectionWithSideInput`](http://spotify.github.io/scio/#com.spotify.scio.values.SCollectionWithSideInput) for replicating small `SCollection`s to all left-hand side values in a large `SCollection`.
- [`SCollectionWithSideOutput`](http://spotify.github.io/scio/#com.spotify.scio.values.SCollectionWithSideOutput) for output to multiple SCollections.
- [`WindowedSCollection`](http://spotify.github.io/scio/#com.spotify.scio.values.WindowedSCollection) for accessing window information.
- [`SCollectionWithFanout`](http://spotify.github.io/scio/#com.spotify.scio.values.SCollectionWithFanout) and [`SCollectionWithHotKeyFanout`](http://spotify.github.io/scio/#com.spotify.scio.values.SCollectionWithHotKeyFanout) for fanout of skewed data.

Scio also offers some additional features.

- Each worker can pull files from Google Cloud Storage via [`DistCache`](http://spotify.github.io/scio/#com.spotify.scio.values.DistCache) to be used in transforms locally, similar to Hadoop distributed cache. See [DistCacheExample.scala](https://github.com/spotify/scio/blob/master/scio-examples/src/main/scala/com/spotify/scio/examples/extra/DistCacheExample.scala).
- Type safe BigQuery IO via Scala macros. Case classes and converters are generated at compile time based on BQ schema. This eliminates the error prone process of handling generic JSON objects. See [TypedBigQueryTornadoes.scala](https://github.com/spotify/scio/blob/master/scio-examples/src/main/scala/com/spotify/scio/examples/extra/TypedBigQueryTornadoes.scala).
- Sinks (`saveAs*` methods) return `Future[Tap[T]]` that can be opened either in another pipeline as `SCollection[T]` or directly as `Iterator[T]` once the current pipeline completes. This enables complex pipeline orchestration. See [WordCountOrchestration.scala](https://github.com/spotify/scio/blob/master/scio-examples/src/main/scala/com/spotify/scio/examples/extra/WordCountOrchestration.scala).

# Artifacts

Scio includes the following artifacts:

- `scio-core`: core library
- `scio-test`: test utilities, add to your project as a "test" dependency
- `scio-bigquery`: Add-on for Bigquery, included in `scio-core` but can also be used standalone
- `scio-bigtable`: Add-on for Bigtable
- `scio-extra`: Extra utilities for working with collections, Breeze, etc.
- `scio-hdfs`: Add-on for HDFS

To access HDFS from a Scio job, Hadoop configuration files (`core-site.xml`, `hdfs-site.xml`, etc.) must be present in `src/main/resources` and `--network` should be set to one that has access to the Hadoop cluster.

# License

Copyright 2016 Spotify AB.

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
