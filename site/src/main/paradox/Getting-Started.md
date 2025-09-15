# Getting Started

First install the [Google Cloud SDK](https://cloud.google.com/sdk/) and create a [Google Cloud Storage](https://cloud.google.com/storage/) bucket for your project, e.g. `gs://my-bucket`. Make sure it's in the same region as the [BigQuery](https://cloud.google.com/bigquery/) datasets you want to access and where you want Dataflow to launch workers on GCE.

Scio may need Google Cloud's application default credentials for features like BigQuery. Run the following command to set it up.

```bash
gcloud auth application-default login
```

## Building Scio

Scio is built using [SBT](https://www.scala-sbt.org/). To build Scio and publish artifacts locally, run:

```bash
git clone git@github.com:spotify/scio.git
cd scio
# 'sbt +publishLocal' to cross build for all Scala versions
# 'sbt ++$SCALA_VERSION publishLocal' to build for a specific Scala version
sbt publishLocal
```

More info on cross-building [here](https://www.scala-sbt.org/1.x/docs/Cross-Build.html#Switching+Scala+version).

You can also specify sbt heap size with `-mem`, e.g. `sbt -mem 8192`. Some sensible defaults are stored in `.sbtopts`.

To ensure the project loads and builds successfully, run the following sbt command so that all custom tasks are executed

```bash
sbt compile Test/compile
```

## Running the Examples

You can execute the examples locally from SBT. By default, pipelines will be executed using the @javadoc[DirectRunner](org.apache.beam.runners.direct.DirectRunner) and local filesystem will be used for input and output. Take a look at the @github[examples](/scio-examples/src/main/scala/com/spotify/scio/examples) to find out more.

```
neville@localhost scio $ sbt
[info] ...
> project scio-examples
[info] ...
> runMain com.spotify.scio.examples.WordCount --input=<FILE PATTERN> --output=<DIRECTORY>
```

@@@ note
Unlike Hadoop, Scio or Dataflow input should be file patterns and not directories, i.e. `gs://bucket/path/part-*.txt` and not `gs://bucket/path`. Output on the other hand should be directories just like Hadoop, so `gs://bucket/path` will produce files like `gs://bucket/path/part-00000-of-00005.txt`.
@@@

Use the @javadoc[DataflowRunner](org.apache.beam.runners.dataflow.DataflowRunner) to execute pipelines on Google Cloud Dataflow service using managed resources in the Google Cloud Platform.

```
neville@localhost scio $ sbt
[info] ...
> project scio-examples
[info] ...
> set beamRunners := "DataflowRunner"
[info] ...
> runMain com.spotify.scio.examples.WordCount
--project=<PROJECT ID>
--region=<GCE AVAILABILITY ZONE> --runner=DataflowRunner
--input=<FILE PATTERN> --output=<DIRECTORY>
```

The Cloud Platform `project` refers to its name (not number). GCE availability `region` should be in the same region as the BigQuery datasets and GCS bucket.

By default, only `DirectRunner` is in the library dependencies list. Use `set beamRunners := "<runners>"` to specify additional runner dependencies as a comma separated list, i.e. "DataflowRunner,FlinkRunner".

## SBT project setup

To create a new SBT project using Giter8 [scio-template](https://github.com/spotify/scio-template.g8), simply:

```
sbt new spotify/scio-template.g8
```

Or add the following to your `build.sbt`. Replace the direct and Dataflow runner with ones you wish to use.
The compiler plugin dependency is only needed for the type safe BigQuery API with scala 2.12.

```sbt
libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-test" % scioVersion % Test,
  "org.apache.beam" % "beam-runners-direct-java" % beamVersion % Runtime,
  "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion % Runtime
)

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full)
```

## BigQuery Settings

You may need a few extra settings to use BigQuery queries as pipeline input.

```
sbt -Dbigquery.project=<PROJECT-ID>
```

- `bigquery.project`: GCP project to make BigQuery requests with at compile time.
- `bigquery.secret`: By default the credential in Google Cloud SDK will be used. A JSON secret file can be used instead with `-Dbigquery.secret=secret.json`.

## Options

The following options should be specified when running a job on Google Cloud Dataflow service.

- `--project` - The project ID for your Google Cloud Project. This is required if you want to run your pipeline using the Cloud Dataflow managed service.
- `--region` - The Compute Engine [regional endpoint](https://cloud.google.com/dataflow/docs/resources/locations) for launching worker instances to run your pipeline.

For pipeline execution parameters and optimization, see the following documents.

- [Specifying Pipeline Execution Parameters](https://cloud.google.com/dataflow/pipelines/specifying-exec-params)
- [Service Optimization and Execution](https://cloud.google.com/dataflow/service/dataflow-service-desc)

The defaults should work well for most cases but we sometimes tune the following parameters manually.
- `--workerMachineType` - start with smaller types like `n1-standard-1` and go up if you run into memory problem. `n1-standard-4` works well for a lot of our memory hungry jobs.
- `--maxNumWorkers` - avoid setting it to too high, i.e. 1000 or close to quota, since that reduces available instances for other jobs and more workers means more expensive shuffle.
- `--diskSizeGb` - increase this if you run into disk space problem during shuffle, or alternatively optimize code by replacing `groupByKey` with `reduceByKey` or `sumByKey`.
- `--workerDiskType` - specify SSD for jobs with really expensive shuffles. See a list of disk types [here](https://cloud.google.com/compute/docs/reference/latest/diskTypes). Also see this [page](https://cloud.google.com/compute/docs/disks/performance) about persistent disk size and type.
- `--network` - specify this if you use VPN to communicate with external services, e.g. HDFS on an on-premise cluster.

More Dataflow pipeline specific options available can be found in @javadoc[DataflowPipelineOptions](org.apache.beam.runners.dataflow.options.DataflowPipelineOptions) and super interfaces. Some more useful ones are from @javadoc[DataflowPipelineWorkerPoolOptions](org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions).

@javadoc[DataflowWorkerHarnessOptions#getWorkerCacheMb](org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions#getWorkerCacheMb--) affects side input performance but needs an extra step to enable. See this @ref:[FAQ item](FAQ.md#how-do-i-improve-side-input-performance-).

There are a few more experimental settings that might help specific scenarios:
- `--experiments=shuffle_mode=service` - use external [shuffle service](https://cloud.google.com/dataflow/service/dataflow-service-desc#cloud-dataflow-shuffle) instead of local disk
- `--experiments=enable_custom_bigquery_sink` - new custom sink that works around certain limitations when writing to BigQuery
- `--experiments=worker_region-<REGION>` - use specified Google Cloud region instead of zone for more flexible capacity
