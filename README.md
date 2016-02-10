Scio
====

Scala API for Google Cloud Dataflow

See the [current API documentation](http://spotify.github.io/scio/) for more information.

# Getting Started

First install the [Google Cloud SDK](https://cloud.google.com/sdk/) and create a [Google Cloud Storage](https://cloud.google.com/storage/) bucket for your project, e.g. `gs://my-bucket`. Make sure it's in the same region as the BigQuery datasets you want to access and where you want Dataflow to launch workers on GCE.

Then clone this repository and publish artifacts locally.

```bash
git clone git@github.com:spotify/scio.git
cd scio
sbt publish-local
```

# Running the Examples

You can execute the examples locally from SBT. By default pipelines will be executed using the `DirectPipelineRunner` and local filesystem will be used for input and output.

The `-Dbigquery.project=<BILLING_PROJECT>` argument is required to compile the typed BigQuery example since it triggers BigQuery requests at compile time.

```
neville@localhost scio $ sbt -Dbigquery.project=<BILLING_PROJECT>
[info] ...
> project scio-examples
[info] ...
> runMain com.spotify.scio.examples.WordCount
--input=<INPUT FILE PATTERN>
--output=<OUTPUT DIRECTORY>
```

You can use the `BlockingDataflowPipelineRunner` or `DataflowPipelineRunner` to execute pipelines on Google Cloud Dataflow Service using managed resources in the Google Cloud Platform. `BlockingDataflowPipelineRunner` will block the main process until job completes while `DataflowPipelineRunner` will submit the job and exit immediately.

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

- `bigquery.project`: GCP project to make BigQuery requests with.
- `bigquery.staging_dataset.location`: Geographical location for BigQuery staging dataset, e.g. `US`, `EU`, must be the same as source tables and GCS buckets.

# Options

More Dataflow pipeline specific options available can be found in [`DataflowPipelineOptions`](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/options/DataflowPipelineOptions) and super interfaces. Some more useful ones are from [`DataflowPipelineWorkerPoolOptions`](https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/options/DataflowPipelineWorkerPoolOptions):

- `--numWorkers`: Number of workers to use when executing the Dataflow job.
- `--autoscalingAlgorithm`: [Experimental] The autoscaling algorithm to use for the workerpool. `NONE`: does not change the size of the worker pool. `THROUGHPUT_BASED`: Autoscale the workerpool based on throughput (up to maxNumWorkers). (default=`NONE`)
- `--maxNumWorkers`: [Experimental] The maximum number of workers to use when using workerpool autoscaling. (default=20)
- `--diskSizeGb`: Remote worker disk size, in gigabytes, or 0 to use the default size.
- `--workerMachineType`: Machine type to create Dataflow worker VMs as. See [https://cloud.google.com/compute/docs/machine-types](https://cloud.google.com/compute/docs/machine-types) for a list of valid options. If unset, the Dataflow service will choose a reasonable default.
- `--network`: GCE network for launching workers.

# Scio vs. Spark

The Scio API is modeled after Spark with some minor differences.

- [`SCollection`](http://spotify.github.io/scio/#com.spotify.scio.values.SCollection) is equivalent to Spark's [`RDD`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD).
- [`PairSCollectionFunctions`](http://spotify.github.io/scio/#com.spotify.scio.values.PairSCollectionFunctions) and [`DoubleSCollectionFunctions`](http://spotify.github.io/scio/#com.spotify.scio.values.DoubleSCollectionFunctions) are specialized versions of `SCollection` and equivalent to Spark's [`PairRDDFunctions`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.PairRDDFunctions) and [`DoubleRDDFunctions`](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.DoubleRDDFunctions).
- Execution planning is static and happens before the job is submitted. There is no driver node in a Dataflow cluster and one can only perform the equivalent of Spark _transformations_ (`RDD` &rarr; `RDD`) but not _actions_ (`RDD` &rarr; driver local memory).
- There is no _broadcast_ either but the pattern of `RDD` &rarr; driver via _action_ and driver &rarr; `RDD` via _broadcast_ can be replaced with `SCollection.asSingleTonSideInput` and `SCollection.withSideInputs`.
- There is no `DStream` (continuous series of `RDD`s) like in Spark Streaming. Values in a `SCollection` are windowed based on timestamp and windowing operation. The same API works regardless of batch (single global window by default) or streaming mode. Aggregation type _transformations_ that produce `SCollection`s of a single value under global window will produce one value each window when a non-global window is defined.
- `SCollection` has extra methods for side input, side output, and windowing.
- `SCollection` can be converted to `SCollectionWithAccumulator` (allowing custom counters similar to those in Hadoop), `SCollectionWithSideInput` (replicating small `SCollection`s to all left-hand side values), `SCollectionWithSideOutput` (allowing multiple outputs), or `WindowedSCollection` (allowing access to window information).
- Each worker can pull files from Google Cloud Storage via `DistCache` to be used in transforms locally, similar to Hadoop distributed cache.

# Artifacts

Scio includes the following artifacts:

- `scio-core`: core library
- `scio-test`: test utilities, add to your project as a "test" dependency
- `scio-bigquery`: Add-on for Bigquery, included in `scio-core` but can also be used standalone
- `scio-bigtable`: Add-on for Bigtable
- `scio-extra`: Extra utilities for working with collections, Breeze, etc.
- `scio-hdfs`: Add-on for HDFS

To access HDFS from a Scio job, Hadoop configuration files (`core-site.xml`, `hdfs-site.xml`, etc.) must be present in `src/main/resources` and `--network` should be set to one that has access to the Hadoop cluster.
