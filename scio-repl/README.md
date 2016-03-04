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
Loading ...
Scio context available as 'sc'
Welcome to Scio REPL!

scio>
```
Scio context is available as `sc`, this is the starting point, use `tab` completion, history and
all the other goods of REPL to play around.

# Tutorial

## Local pipeline

Let's start with simple local-mode word count example:

```
scio> val wordCount = sc.textFile("README.md").flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty)).countByValue.map(_.toString).saveAsTextFile("/tmp/local_wordcount")
scio> sc.close
scio> readme_wc.waitForResult().value.take(3).foreach(println)
(but,4)
(via,4)
(Hadoop,6)
```

Make sure to have text file `README.md` in current directory (the directory where you start REPL from).
 This example counts words in local file using local Dataflow runner and outputs result in local file.
The pipeline/computation starts on `sc.close`. The last command take 3 lines from results and prints
 them.

## Local pipeline ++

In the next example we will spice things up a bit - and read data from GCS:

```
scio> :newScio
scio> val shakespeare = sc.textFile("gs://dataflow-samples/shakespeare/*")
scio> val wordCount = shakespeare.flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty)).countByValue.map(_.toString).saveAsTextFile("/tmp/gcs-wordcount")
scio> sc.close
scio> wordCount.waitForResult().value.take(3).foreach(println)
(frown'st,1)
(comfortable,13)
(diversity,1)
```

We are still performing computations locally, but data comes from Google Cloud Storage (it might as
 well be BigTable, BigQuery etc). This example may take a minute or two due to transfer between GCS
 and your machine.

## Dataflow service pipeline

To create a ScioContext for Google Cloud Dataflow service pass Dataflow pipeline options on
 REPL startup - each `:newScio` will use those arguments for new Scio contexts. For example:

```bash
$ java -jar scio-repl/target/scala-2.11/scio-repl*-fat.jar \
> --project=<project-id> \
> --stagingLocation=<stagin-dir> \
> --runner=BlockingDataflowPipelineRunner
Loading ...
Scio context available as 'sc'
Welcome to Scio REPL!

scio> val shakespeare = sc.textFile("gs://dataflow-samples/shakespeare/*")
scio> val wordCount = shakespeare.flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty)).countByValue.map(_.toString).saveAsTextFile("gs://<gcs-output-dir>")
scio> sc.close
scio> wordCount.waitForResult().value.take(3).foreach(println)
(decreased,1)
('shall',2)
(War,4)
```

In this case we are reading data from GCS and performing computation in Dataflow service. Last line
 is an example fo reading data from GCS file/tap.

## Adhoc local mode

Sometimes you may start in distributed mode get small enough results, and play around the results
 in local mode. At any point in time you can create a local Scio context using
 `:newLocalScio <name>` and use it to perform local computations.

```
scio> :newLocalScio lsc
Local Scio context available as 'lsc'
```

## BigQuery example

In this example let's try to read some data from BigQuery and massage it in Dataflow. We shall find
 relationship between months and numbers of detected tornado from public Dataflow dataset:

```bash
$ java -jar -Dbigquery.project=<project-id> \
> scio-repl/target/scala-2.11/scio-repl*-fat.jar \
> --project=<project-id> \
> --stagingLocation=<stagin-dir> \
> --runner=BlockingDataflowPipelineRunner
Loading ...
Scio context available as 'sc'
Welcome to Scio REPL!

scio> val tornados = sc.bigQuerySelect("SELECT tornado, month FROM [clouddataflow-readonly:samples.weather_stations]")
scio> val forcast = tornados.flatMap(r => if (r.getBoolean("tornado")) Seq(r.getInt("month")) else Nil).countByValue().map(kv => TableRow("month" -> kv._1, "tornado_count" -> kv._2))
scio> val someData = forcast.take(3).materialize
scio> sc.close
scio> someData.waitForResult().value.foreach(println)
{month=4, tornado_count=5}
{month=3, tornado_count=6}
{month=5, tornado_count=6}
```

In this example we combine power of BigQuery and flexibility of Dataflow. We first query BigQuery
 table, perform a couple of transformations to find out relationship between months and number of
 torandos. We take (`take(3)`) some data back locally (`materialize`) to check the results.

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
Loading ...
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
Loading ...
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

## Using BigQuery client

Whenever possible leverage BigQuery! `@BigQueryType` annotations enable type safe and civilized
 integration with BigQuery inside Scio. Here is example of using `@BigQueryType` annotations and
 BigQuery client to read typed data.

```bash
$ java -jar -Dbigquery.project=<project-id> \
> scio-repl/target/scala-2.11/scio-repl*-fat.jar
Loading ...
Scio context available as 'sc'
Welcome to Scio REPL!

scio> @BigQueryType.fromQuery("SELECT tornado, month FROM [clouddataflow-readonly:samples.weather_stations]") class Row
scio> val bqc = BigQueryClient()
scio> val tornados = bqc.getTypedRows[Row]()
scio> tornados.next.month
res5: Option[Long] = Some(5)
```
