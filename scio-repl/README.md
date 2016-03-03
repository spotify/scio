Scio REPL
=========

# Getting started

The Scio REPL is an extension of the Scala REPL, with added functionality that allows you to
interactively experiment with Scio. Think of it as a playground to try out things.

# Quick start

The easiest way to start with Scio REPL is to assembly jar and run it:

```bash
$ sbt 'project scio-repl' assembly
$ java -jar scio-repl/target/scala-2.11/scio-repl*-fat.jar
Starting up ...
Scio context available as 'sc'
Welcome to Scio REPL!
scio>
```
Scio context is available as `sc`, this is the starting point, use `tab` completion, history and
all the other goods of REPL to play around in local mode.

# Tutorial

Start simple pipeline from

```
scio> val noTwoS = sc.parallelize(List(1,2,3)).filter( _ != 2 ).map( "I like " + _ ).saveAsTextFile("/tmp/hate-2s")
twoS: scala.concurrent.Future[com.spotify.scio.io.Tap[String]] = scala.concurrent.impl.Promise$DefaultPromise@270ab7bc
scio> val result = sc.close
[main] INFO com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner - Executing pipeline using the DirectPipelineRunner.
[main] INFO com.google.cloud.dataflow.sdk.runners.DirectPipelineRunner - Pipeline execution complete.
result: com.spotify.scio.ScioResult = com.spotify.scio.ScioResult@220a5163
scio> result.state
res20: com.google.cloud.dataflow.sdk.PipelineResult.State = DONE
scio> noTwoS.waitForResult().value.foreach(println)
I like 3
I like 1
```

To create a ScioContext for Google Cloud Dataflow service pass Dataflow pipeline options on
 REPL startup - each `:newScio` will use those arguments for new Scio contexts. For example:

```bash
$ java -jar scio-repl/target/scala-2.11/scio-repl*-fat.jar \
> --project=<project-id> \
> --stagingLocation=<stagin-dir> \
> --runner=BlockingDataflowPipelineRunner
Starting up ...
Scio context available as 'sc'
Welcome to Scio REPL!
scio> sc.parallelize(List(1,2,3)).map( _.toString ).saveAsTextFile("gs://<output-dir>")
scio> val result = sc.close
```

At any point in time you can always create a local Scio context using `:newLocalScio <name>`:

```
scio> :newLocalScio lsc
Local Scio context available as 'lsc'
```

# Tips

## Use `:paste` for multiline code

While in the REPL, use `:paste` magic command to past/write multi line code

```
scio> :paste
// Entering paste mode (ctrl-D to finish)
def myWordFilter(word: String) = word.startsWith("rav")
val importantNames = sc.parallelize(List("ravioli", "rav", "jbx")).filter(myWordFilter)
// Exiting paste mode, now interpreting.
myWordFilter: (word: String)Boolean
importantNames: com.spotify.scio.values.SCollection[String] = com.spotify.scio.values.SCollectionImpl@75882ac2
scio> importantNames.saveAsTextFile("/tmp/ravs")
scio> sc.close
```

## `DataflowPipelineRunner` = more interactive Dataflow service

When using REPL and Dataflow service consider using the non-blocking `DataflowPipelineRunner` to get
more interactive experience. To start:

```bash
java -jar scio-repl/target/scala-2.11/scio-repl*-fat.jar \
> --project=<project-id> \
> --stagingLocation=<stagin-dir> \
> --runner=DataflowPipelineRunner
Starting up ...
Scio context available as 'sc'
Welcome to Scio REPL!
scio> sc.parallelize(List(1,2,3)).map( _.toString ).saveAsTextFile("gs://<output>")
res1: scala.concurrent.Future[com.spotify.scio.io.Tap[String]] = scala.concurrent.impl.Promise$DefaultPromise@1399ad68
scio> val futureResult = sc.close
[main] INFO com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner - Executing pipeline on the Dataflow Service, which will have billing implications related to Google Compute Engine usage and other Google Cloud Services.
[main] INFO com.google.cloud.dataflow.sdk.util.PackageUtil - Uploading 28 files from PipelineOptions.filesToStage to staging location to prepare for execution.
[main] INFO com.google.cloud.dataflow.sdk.util.PackageUtil - Uploading PipelineOptions.filesToStage complete: 2 files newly uploaded, 26 files cached
Dataflow SDK version: 1.4.0
scio> futureResult.state
res4: com.google.cloud.dataflow.sdk.PipelineResult.State = RUNNING
```

When you decide to close the Scio context, you won't have to wait for the job to finish but instead
get control back in REPL - use futures/taps to check for results, triggers and plumbing.

## Use multiple Scio contexts

You can use multiple Scio context objects to work with multiple pipelines at the same time, simply
use magic `:newScio <context name>`, for example:

```
scio> :newScio c1
Scio context available as 'c1'
scio> :newScio c2
Scio context available as 'c2'
scio> :newLocalScio lc
Scio context available as 'lc'
```

You can use those in combination with `DataflowPipelineRunner` to run multiple pipelines at the same
 session or pipe them together etc.

## Asynchronously close Scio context

Especially if working in distributed mode one may want to schedule a job and continue experimenting
in REPL. `DataflowPipelineRunner` is helpful but just the startup of the job can take minutes.
`asyncClose` can be used to asynchronous close both `BlockingDataflowPipeline` and `DataflowPipeline`
 runners, for example:

```bash
$ java -jar -Dorg.slf4j.simpleLogger.logFile=<async-exec-log-file> \
> scio-repl/target/scala-2.11/scio-repl*-fat.jar \
> --project=<project-id> \
> --stagingLocation=<stagin-dir> \
> --runner=DataflowPipelineRunner
Starting up ...
Scio context available as 'sc'
Welcome to Scio REPL!
scio> sc.parallelize(List(1,2,3)).map( _.toString ).saveAsTextFile("gs://<output-dir>")
scio> val result = sc.asyncClose()
scio> result.isCompleted
res3: Boolean = false
scio> :newScio nextSc
```

Keep in mind tho that currently `BlockingDataflowPipelineRunner` is still pretty noise on stdout,
thus, currently, it's recommended to use `DataflowPipelineRunner` with `asyncClose`.

## Use BigQuery macros

@BigQueryType annotations enable type safe and. and civilized integration with BigQuery inside Scio.
Use it inside REPL when possible. For example:

```bash
java -jar -Dbigquery.project=<project-id> scio-repl/target/scala-2.11/scio-repl*-fat.jar
Starting up ...
Scio context available as 'sc'
Welcome to Scio REPL!
scio> @BigQueryType.fromQuery("SELECT tornado, month FROM [publicdata:samples.gsod]") class Row
defined class Row
defined object Row
```
