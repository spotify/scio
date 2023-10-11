# REPL

The Scio REPL is an extension of the Scala REPL, with added functionality that allows you to interactively experiment with Scio. Think of it as a playground to try out things.

## Quick start

You can either install Scio REPL via our [Homebrew tap](https://github.com/spotify/homebrew-public) on a Mac or download the pre-built jar on other platforms.

### Homebrew

```bash
brew tap spotify/public
brew install scio
scio-repl
```

### Pre-built jar

To download pre-built jar of Scio REPL, find version you are interested in on the [release page](https://github.com/spotify/scio/releases), and download the REPL jar from `Downloads` section.

```
$ java -jar scio-repl-<version>.jar
Welcome to
                 _____
    ________________(_)_____
    __  ___/  ___/_  /_  __ \
    _(__  )/ /__ _  / / /_/ /
    /____/ \___/ /_/  \____/   version 0.7.0

Using Scala version 2.12.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_144)
Type in expressions to have them evaluated.
Type :help for more information.

BigQuery client available as 'bq'
Scio context available as 'sc'

scio>
```

A @scaladoc[ScioContext](com.spotify.scio.ScioContext) is created on REPL startup as `sc` and a starting point to most operations. Use `tab` completion, history and other REPL goodies to play around.

### Start from SBT console

```
$ git clone git@github.com:spotify/scio.git
Cloning into 'scio'...
remote: Counting objects: 9336, done.
remote: Compressing objects: 100% (275/275), done.
remote: Total 9336 (delta 139), reused 0 (delta 0), pack-reused 8830
Receiving objects: 100% (9336/9336), 1.76 MiB | 0 bytes/s, done.
Resolving deltas: 100% (3509/3509), done.
Checking connectivity... done.
$ cd scio
$ sbt scio-repl/run
```

### Build REPL jar manually

You can also build REPL jar from source.

```
$ git clone git@github.com:spotify/scio.git
Cloning into 'scio'...
remote: Counting objects: 9336, done.
remote: Compressing objects: 100% (275/275), done.
remote: Total 9336 (delta 139), reused 0 (delta 0), pack-reused 8830
Receiving objects: 100% (9336/9336), 1.76 MiB | 0 bytes/s, done.
Resolving deltas: 100% (3509/3509), done.
Checking connectivity... done.
$ cd scio
$ sbt scio-repl/assembly
```

### sbt project from scio-template

Projects generated from [scio-template.g8](https://github.com/spotify/scio-template.g8) have built-in REPL. Run `sbt repl/run` from the project root.

## Tutorial

### Local pipeline

Let's start with simple local-mode word count example:

```scala mdoc:invisible
// The REPL loads the following for you.
import com.spotify.scio._

def sc: ScioContext = ???
```

```scala mdoc:compile-only
val wordCount = sc
    .textFile("README.md")
    .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
    .countByValue
    .map(_.toString)
    .saveAsTextFile("/tmp/local_wordcount")

val scioResult = sc.run().waitUntilDone()

val values = scioResult.tap(wordCount).value.take(3)
```

Make sure `README.md` is in the current directory. This example counts words in local file using a local runner (@javadoc[DirectRunner](org.apache.beam.runners.direct.DirectRunner) and writes result in a local file. The pipeline and actual computation starts on `sc.run()`. The last command take 3 lines from results and prints them.

### Local pipeline ++

In the next example we will spice things up a bit and read data from GCS:

```
:newScio
```

```scala mdoc:reset:invisible
import com.spotify.scio._

def sc: ScioContext = ???
```

```scala mdoc:compile-only
val shakespeare = sc.textFile("gs://dataflow-samples/shakespeare/hamlet.txt")

val wordCount = shakespeare
    .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
    .countByValue
    .map(_.toString)
    .saveAsTextFile("/tmp/gcs-wordcount")

val result = sc
    .run()
    .waitUntilDone()
    .tap(wordCount)
    .value
    .take(3)
```

Each Scio context is associated with one and only one pipeline. The previous instance of `sc` was used for the local pipeline example and cannot be reused anymore. The first magic command, `:newScio` creates a new context as `sc`. The pipeline still performs computation locally, but reads data from Google Cloud Storage (it could also be BigQuery, Datastore, etc). This example may take a bit longer due to additional network overhead.

### Dataflow service pipeline

To create a Scio context for Google Cloud Dataflow service, add Dataflow pipeline options when starting the REPL. The same options will also be used by `:newScio` when creating new context. For example:

```
$ java -jar scio-repl-0.7.0.jar \
> --project=<project-id> \
> --stagingLocation=<stagin-dir> \
> --runner=DataflowRunner
Welcome to
                 _____
    ________________(_)_____
    __  ___/  ___/_  /_  __ \
    _(__  )/ /__ _  / / /_/ /
    /____/ \___/ /_/  \____/   version 0.7.0

Using Scala version 2.12.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_144)
Type in expressions to have them evaluated.
Type :help for more information.

BigQuery client available as 'bq'
Scio context available as 'sc'
```

```scala mdoc:reset:invisible
import com.spotify.scio._

def sc: ScioContext = ???
```

```scala mdoc:compile-only
val shakespeare = sc.textFile("gs://dataflow-samples/shakespeare/*")

val wordCount = shakespeare
    .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
    .countByValue
    .map(_.toString)
    .saveAsTextFile("gs://<gcs-output-dir>")

val result = sc
    .run()
    .waitUntilDone()
    .tap(wordCount)
    .value
    .take(3)
```

In this case we are reading data from GCS and performing computation in GCE virtual machines managed by Dataflow service. The last line is an example of reading data from GCS files to local memory after a context is closed. Most write operations in Scio return `Future[Tap[T]]` where a @scaladoc[Tap[T]](com.spotify.scio.io.Tap) encapsulates some dataset that can be re-opened in another context or directly.

Use `:scioOpts` to view or update Dataflow options inside the REPL. New options will be applied the next time you create a context.

### Ad-hoc local mode

You may start the REPL in distributed mode and run pipelines to aggregate from large datasets, and play around the results in local mode. You can create a local Scio context any time with `:newLocalScio <name>` and use it for local computations.

```
scio> :newLocalScio lsc
Local Scio context available as 'lsc'
```

### BigQuery example

In this example we will read some data from BigQuery and process it in Dataflow. We shall count number of tornadoes per month from a public sample dataset. Scio will do its best to find your configured Google Cloud project, but you can also specify it explicitly via `-Dbigquery.project` option.

```
$ java -jar -Dbigquery.project=<project-id> scio-repl-0.7.0.jar \
> --project=<project-id> \
> --stagingLocation=<stagin-dir> \
> --runner=DataflowRunner
Welcome to
                 _____
    ________________(_)_____
    __  ___/  ___/_  /_  __ \
    _(__  )/ /__ _  / / /_/ /
    /____/ \___/ /_/  \____/   version 0.7.0

Using Scala version 2.12.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_144)
Type in expressions to have them evaluated.
Type :help for more information.

BigQuery client available as 'bq'
Scio context available as 'sc'
```

```scala mdoc:reset:invisible
import com.spotify.scio._
import com.spotify.scio.bigquery._

def sc: ScioContext = ???
```

```scala mdoc:compile-only
val tornadoes = sc.bigQuerySelect(Query("SELECT tornado, month FROM [clouddataflow-readonly:samples.weather_stations]"))
 
val counts = tornadoes
    .flatMap(r => if (r.getBoolean("tornado")) Seq(r.getLong("month")) else Nil)
    .countByValue
    .map(kv => TableRow("month" -> kv._1, "tornado_count" -> kv._2))
    .take(3)
    .materialize

val result = sc
    .run()
    .waitUntilDone()
    .tap(counts)
    .value
```

In this example we combine power of BigQuery and flexibility of Dataflow. We first query BigQuery table, perform a couple of transformations and take (`take(3)`) some data back locally (`materialize`) to view the results.

### BigQuery project id

Scio REPL will do its best to find your configured Google Cloud project, without the need to explicitly specifying `bigquery.project` property. It will search for project-id in this specific order:

1. `bigquery.project` java system property
2. `GCLOUD_PROJECT` java system property
3. `GCLOUD_PROJECT` environmental variable
4. [gcloud](https://cloud.google.com/sdk/gcloud/) config files:
  1. `scio` [named configuration](https://cloud.google.com/sdk/gcloud/reference/topic/configurations)
  2. default configuration

This means that you can always set `bigquery.project` and it will take precedence over other configurations. Read more about gcloud configuration [here](https://cloud.google.com/sdk/gcloud/reference/config/).

### I/O Commands

There are few built-in commands for simple file I/O.

```scala mdoc
import scala.reflect._
import kantan.csv._

// Read from an Avro, text, CSV or TSV file on local filesystem or GCS.
def readAvro[T : ClassTag](path: String): Iterator[T] = ???
def readText(path: String): Iterator[String] = ???
def readCsv[T: RowDecoder](path: String,
                           sep: Char = ',',
                           header: Boolean = false): Iterator[T] = ???
def readTsv[T: RowDecoder](path: String,
                           sep: Char = '\t',
                           header: Boolean = false): Iterator[T] = ???

// Write to an Avro, text, CSV or TSV file on local filesystem or GCS.
def writeAvro[T: ClassTag](path: String, data: Seq[T]): Unit = ???
def writeText(path: String, data: Seq[String]): Unit = ???
def writeCsv[T: RowEncoder](path: String, data: Seq[T],
                            sep: Char = ',',
                            header: Seq[String] = Seq.empty): Unit = ???
def writeTsv[T: RowEncoder](path: String, data: Seq[T],
                            sep: Char = '\t',
                            header: Seq[String] = Seq.empty): Unit = ???
```

## Tips

### Multi-line code

While in the REPL, use `:paste` magic command to paste or write multi-line code

```
:paste
```

```scala mdoc:reset:invisible
import com.spotify.scio._

def sc: ScioContext = ???
```

```scala mdoc
// Entering paste mode (ctrl-D to finish)
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection

def evenNumber(x: Int): Boolean = x % 2 == 0
def evenNumbers: SCollection[Int] = sc.parallelize(1 to 100).filter(evenNumber)

// Exiting paste mode, now interpreting.

def tap: ClosedTap[String] = evenNumbers.saveAsTextFile("/tmp/even")

def result = sc.run()
```

### Running jobs asynchronously

When using REPL and Dataflow service consider using the non-blocking @javadoc[DataflowRunner](org.apache.beam.runners.dataflow.DataflowRunner) for a more interactive experience. To start:

```
java -jar scio-repl-0.7.0.jar \
> --project=<project-id> \
> --stagingLocation=<stagin-dir> \
> --runner=DataflowRunner
Welcome to
                 _____
    ________________(_)_____
    __  ___/  ___/_  /_  __ \
    _(__  )/ /__ _  / / /_/ /
    /____/ \___/ /_/  \____/   version 0.7.0

Using Scala version 2.12.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_144)
Type in expressions to have them evaluated.
Type :help for more information.

BigQuery client available as 'bq'
Scio context available as 'sc'
```

```scala mdoc:reset:invisible
import com.spotify.scio._

def sc: ScioContext = ???
```

```scala mdoc
import com.spotify.scio.io.ClosedTap

def closedTap: ClosedTap[String] = sc
    .parallelize(1 to 100)
    .map( _.toString )
    .saveAsTextFile("gs://<output>")

def result = sc.run()
// [main] INFO org.apache.beam.runners.dataflow.DataflowRunner - Executing pipeline on the Dataflow Service, which will have billing implications related to Google Compute Engine usage and other Google Cloud Services.
// [main] INFO org.apache.beam.runners.dataflow.util.PackageUtil - Uploading 3 files from PipelineOptions.filesToStage to staging location to prepare for execution.
// [main] INFO org.apache.beam.runners.dataflow.util.PackageUtil - Uploading PipelineOptions.filesToStage complete: 2 files newly uploaded, 1 files cached
// Dataflow SDK version: 2.9.0

def state = result.state
```

Note that now `sc.run()` doesn't block and wait until job completes and gives back control of the REPL right away. Use @scaladoc[ScioExecutionContext](com.spotify.scio.ScioExecutionContext) to check for progress, results and orchestrate jobs.

### Multiple Scio contexts

You can use multiple Scio context objects to work with several pipelines at the same time, simply
use magic `:newScio <context name>`, for example:

```
scio> :newScio c1
Scio context available as 'c1'
scio> :newScio c2
Scio context available as 'c2'
scio> :newLocalScio lc
Scio context available as 'lc'
```

You can use those in combination with `DataflowRunner` to run multiple pipelines in the same session or wire them with for comprehension over futures.

### BigQuery client

Whenever possible leverage BigQuery! @scaladoc[@BigQueryType](com.spotify.scio.bigquery.types.BigQueryType) annotations enable type safe and civilized
 integration with BigQuery inside Scio. Here is example of using the annotations and BigQuery client to read and write typed data directly without Scio context.

```
$ java -jar -Dbigquery.project=<project-id> scio-repl-0.7.0.jar
Welcome to
                 _____
    ________________(_)_____
    __  ___/  ___/_  /_  __ \
    _(__  )/ /__ _  / / /_/ /
    /____/ \___/ /_/  \____/   version 0.7.0

Using Scala version 2.12.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_144)
Type in expressions to have them evaluated.
Type :help for more information.

BigQuery client available as 'bq'
Scio context available as 'sc'
```

```scala mdoc:reset:invisible
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.client._

def bq: BigQuery = ???
```

```scala mdoc
@BigQueryType.fromQuery("SELECT tornado, month FROM [clouddataflow-readonly:samples.weather_stations]") class Row

def tornadoes = bq.getTypedRows[Row]()

def result = tornadoes.next().month

def write = bq.writeTypedRows("project-id:dataset-id.table-id", tornadoes.take(100).toList)
```

### Out of memory exception

In case of OOM exceptions, like for example:

```scala mdoc:reset:invisible
import com.spotify.scio._
import com.spotify.scio.io._

def sc: ScioContext = ???
```

```scala mdoc
def closedTap: ClosedTap[String] = ???

def result = sc.run().waitUntilDone().tap(closedTap).value.next()
// Exception in thread "main"
// Exception: java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler in thread "main"
```

simply increase the size of the heap - be reasonable about the amount of data and heap size though.

Example of REPL startup with 2GiB of heap size:

```
$ java -Xmx2g -jar scio-repl-0.7.0.jar
Welcome to
                 _____
    ________________(_)_____
    __  ___/  ___/_  /_  __ \
    _(__  )/ /__ _  / / /_/ /
    /____/ \___/ /_/  \____/   version 0.7.0

Using Scala version 2.12.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_144)

Type in expressions to have them evaluated.
Type :help for more information.

BigQuery client available as 'bq'
Scio context available as 'sc'
```

```scala
Runtime.getRuntime().maxMemory()
// res1: Long = 1908932608
```

### What is the type of an expression?

Use the built in `:t`! `:t` displays the type of an expression without evaluating it. Example:

```
scio> :t sc.textFile("README").flatMap(_.split("[^a-zA-Z']+")).filter(_.nonEmpty).map(_.length)
com.spotify.scio.values.SCollection[Int]
```

Learn more about magic keywords via `scio> :help`
