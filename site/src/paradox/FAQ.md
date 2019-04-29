# FAQ

@@toc { depth=2 }

### General questions

#### What's the status of Scio?

Scio is widely being used for production data pipelines at Spotify and is our preferred framework for building new pipelines on Google Cloud. We run Scio on [Google Cloud Dataflow](https://cloud.google.com/dataflow/) service in both batch and streaming modes. However it's still under heavy development and there might be minor breaking API changes from time to time.

#### Who's using Scio?

Spotify uses Scio for all new data pipelines running on Google Cloud Platform, including music recommendation, monetization, artist insights and business analysis. We also use BigQuery, Bigtable and Datastore heavily with Scio. We use Scio in both batch and streaming mode.

As of mid 2017, there're 200+ developers and 700+ production pipelines. The largest batch job we've seen uses 800 n1-highmem-32 workers (25600 CPUs, 166.4TB RAM) and processes 325 billion rows from Bigtable (240TB). We also have numerous jobs that process 10TB+ of BigQuery data daily. On the streaming front, we have many jobs with 30+ n1-standard-16 workers (480 CPUs, 1.8TB RAM) and SSD disks for real time machine learning or reporting.

For a incomplete list of users, see the [[Powered By]] page.

#### What's the relationship between Scio and Apache Beam?

Scio is a Scala API built on top of [Apache Beam](https://beam.apache.org/)'s Java SDK. Scio aims to offer a concise, idiomatic Scala API for a subset of Beam's features, plus extras we find useful, like REPL, type safe BigQuery, and IO taps.

#### What's the relationship between Scio and Google Cloud Dataflow?

Scio (version before 0.3.0) was originally built on top of Google Cloud Dataflow's [Java SDK](https://github.com/GoogleCloudPlatform/DataflowJavaSDK). Google donated the code base to Apache and renamed it Beam. Cloud Dataflow became one of the supported runners, alongside Apache Flink & Apache Spark. Scio 0.3.x is built on top of Beam 0.6.0 and 0.4.x is built on top of Beam 2.x. Many users run Scio on the Dataflow runner today.

#### How does Scio compare to Scalding or Spark?

Check out the wiki page on [[Scio, Scalding and Spark]]. Also check out [Big Data Rosetta Code](https://github.com/spotify/big-data-rosetta-code) for some snippets.

#### What are GCE availability zone and GCS bucket location?

- GCE [availability zone](https://cloud.google.com/compute/docs/zones) is where the Google Cloud Dataflow service spins up VM instances for your job, e.g. `us-east1-a`.
- Each GCS bucket (`gs://bucket`) has a [storage class](https://cloud.google.com/storage/docs/storage-classes) and [bucket location](https://cloud.google.com/storage/docs/bucket-locations) that affects availability, latency and price. The location should be close to GCE availability zone. Dataflow uses `--stagingLocation` for job jars, temporary files and BigQuery I/O.

### Programming questions

#### How do I setup a new SBT project?

Read the @ref:[documentation](Getting-Started.md#sbt-project-setup).

#### How do I deploy Scio jobs to Dataflow?

When developing locally, you can do `sbt "runMain MyClass ...` or just `runMain MyClass ...` in the SBT console without building any artifacts.

When deploying to the cloud, we recommend using [sbt-pack](https://github.com/xerial/sbt-pack) or [sbt-native-packager](https://github.com/sbt/sbt-native-packager) plugin instead of [sbt-assembly](https://github.com/sbt/sbt-assembly). Unlike assembly, they pack dependency jars in a directory instead of merging them, so that we don't have to deal with merge strategy and dependency jars can be cached by Dataflow service.

At Spotify we pack jars with sbt-pack, build docker images with [sbt-docker](https://github.com/marcuslonnberg/sbt-docker) together with orchestration components e.g. [Luigi](https://github.com/spotify/luigi) or [Airflow](https://github.com/apache/incubator-airflow) and deploy them with [Styx](https://github.com/spotify/styx).

#### How do I use the SNAPSHOT builds of Scio?

Commits to Scio master are automatically published to Sonatype via continuous integration. To use the latest SNAPSHOT artifact, add the following line to your `build.sbt`.

```scala
resolvers += Resolver.sonatypeRepo("snapshots")
```

Or you can configure SBT globally by adding the following to `~/.sbt/1.0/global.sbt`.

```scala
resolvers ++= Seq(
  Resolver.sonatypeRepo("snapshots")
  // other resolvers
)
```

#### How do I unit test pipelines?

Any Scala or Java unit testing frameworks can be used with Scio but we provide some utilities for [ScalaTest](http://www.scalatest.org/).

- @scaladoc[PipelineTestUtils](com.spotify.scio.testing.PipelineTestUtils) - utilities for testing parts of a pipeline
- @scaladoc[JobTest](com.spotify.scio.testing.JobTest$) - for testing pipelines end-to-end with complete arguments and IO coverage
- @scaladoc[SCollectionMatchers](com.spotify.scio.testing.SCollectionMatchers) - ScalaTest matchers for `SCollection`
- @scaladoc[PipelineSpec](com.spotify.scio.testing.PipelineSpec) - shortcut for ScalaTest `FlatSpec` with utilities and matchers

The best place to find example useage of `JobTest` and `SCollectionMatchers` are their respective tests in @github[JobTestTest](/scio-test/src/test/scala/com/spotify/scio/testing/JobTestTest.scala) and @github[SCollectionMatchersTest](/scio-test/src/test/scala/com/spotify/scio/testing/SCollectionMatchersTest.scala).
For more examples see:

- @github[scio-examples](/scio-examples/src/test/scala/com/spotify/scio/examples)
- https://github.com/spotify/big-data-rosetta-code/tree/master/src/test/scala/com/spotify/bdrc/testing

#### How do I combine multiple input sources?

How do I combine multiple input sources, e.g. different BigQuery tables, files located in different GCS buckets?
You can combine `SCollection`s from different sources into one using the companion method `SCollection.unionAll`, for example:

```scala
val (sc, args) = ContextAndArgs(cmdlineArgs)

val collections = Seq("gs://bucket1/data/*.avro", "gs://bucket2/data/*.avro")
    .map(sc.avroFile[SchemaType](_))
val all = SCollection.unionAll(collections)
```

#### How do I log in a job?

You can log in a Scio job with most common logging libraries but `slf4j` is included as a dependency. Define the logger instance as a member of the job `object` and use it inside a lambda.

```scala
import org.slf4j.LoggerFactory
object MyJob {
  private val logger = LoggerFactory.getLogger(this.getClass)
  def main(cmdlineArgs: Array[String]): Unit = {
    // ...
    sc.parallelize(1 to 100)
      .map { i =>
        logger.info(s"Element $i")
        i * i
      }
    // ...
  }
}
```

#### How do I use Beam's Java API in Scio?

Scio exposes a few things to allow easy integration with native Beam Java API, notably:

- `ScioContext#customInput` to apply a `PTransform[_ >: PBegin, PCollection[T]]` (source) and get a `SCollection[T]`.
- `SCollection#applyTransform` to apply a `PTransform[_ >: PCollection[T], PCollection[U]]` and get a `SCollection[U]`
- `SCollection#saveAsCustomOutput` to apply a `PTransform[_ >: PCollection[T], PDone]` (sink) and get a `ClosedTap[T]`.

See @extref[BeamExample.scala](example:BeamExample) for more details. Custom I/O can also be tested via the @scaladoc[`JobTest`](com.spotify.scio.testing.JobTest$) harness.

#### What are the different types of joins and performance implication?

- Inner (`a.join(b)`), left (`a.leftOuterJoin(b)`), outer (`a.fullOuterJoin(b)`) performs better with a large LHS. So `a` should be the larger data set with potentially more hot keys, i.e. key with many values. Every key-value pair from every input is shuffled.
- `join`/`leftOuterJoin` may be replaced by `hashJoin`/`leftHashJoin` if the RHS is small enough to fit in memory (e.g. &lt; 1GB). The RHS is used as a multi-map side input for the LHS. No shuffle is performed.
- Consider `skewedJoin` if some keys on the LHS are extremely hot.
- Consider `sparseOuterJoin` if you want a full outer join where RHS is much smaller than LHS, but may not fit in memory.
- Consider `cogroup` if you need to access value groups of each key.
- [`MultiJoin`](https://spotify.github.io/scio/api/com/spotify/scio/util/MultiJoin$.html) supports inner, left, outer join and cogroup of up to 22 inputs.
- For multi-joins larger inputs should be on the left, e.g. `size(a) >= size(b) >= size(c) >= size(d)` in `MultiJoin(a, b, c, d)`.
- Check out these [slides](http://www.lyh.me/slides/joins.html) for more information on joins.
- Also see this section on [Cloud Dataflow Shuffle](https://cloud.google.com/dataflow/service/dataflow-service-desc#cloud-dataflow-shuffle) service.

#### How to create Dataflow job template?

For Apache Beam based Scio (version >= `0.3.0`) use `DataflowRunner` and specify `templateLocation` option. For example in CLI `--templateLocation=gs://<bucket>/job1`. Read more about templates [here](https://cloud.google.com/dataflow/docs/templates/overview).

#### How do I cancel a job after certain time period?

You can wait on the `ScioResult` and call the internal `PipelineResult#cancel()` method if a timeout exception happens.

```scala
val r = sc.close()
import scala.concurrent.duration._
if (Try(r.waitUntilFinish(1.minute)).isFailure) {
  r.internal.cancel()
}
```

#### Why can't I have an SCollection inside another SCollection?

You cannot have an SCollection inside another SCollection, i.e. anything with type `SCollection[SCollection[T]]`. To explain this we have to go back to the relationship between `ScioContext` and `SCollection`. Every `ScioContext` represents a unique pipeline and every `SCollection` represents a stage in the pipeline execution, i.e. the state of the pipeline after some transforms has be applied. We start a pipeline code with `val sc = ...`, create new `SCollection`s with methods on `sc`, e.g. `sc.textFile`, and transform them with methods like `.map`, `.filter`, `.join`. Therefore each `SCollection` can trace its root to one single `sc`. The pipeline is submitted for execution when we call `sc.close()`. Hence we cannot have an `SCollection` inside another `SCollection` just as we cannot have a pipeline inside another pipeline.

### BigQuery questions

#### What is BigQuery dataset location?

- Each BigQuery dataset has a location (e.g. `US`, `EU`) and every table inside are stored in the same location. Tables in a `JOIN` must be from the same region. Also one can only import/export tables to a GCS bucket in the same location. Starting from v0.2.1, Scio will detect the dataset location of a query and create a staging dataset for `ScioContext#bigQuerySelect` and `@BigQueryType.fromQuery`. This location should be the same as that of your `--stagingLocation` GCS bucket. The old `-Dbigquery.staging_dataset.location` flag is removed.

Because of these limitations and performance reasons, make sure `--zone`, `--stagingLocation` and ~~`-Dbigquery.staging_dataset.location`~~ location of BigQuery datasets are consistent.

#### How stable is the type safe BigQuery API?

@ref[Type Safe BigQuery](io/Type-Safe-BigQuery.md) API is considered stable and widely used at Spotify. There are several caveats however:

- Both [legacy](https://cloud.google.com/bigquery/query-reference) and [SQL](https://cloud.google.com/bigquery/sql-reference/) syntax are supported although the SQL syntax is **highly recommended**
- The system will detect legacy or SQL syntax and choose the correct one
- To override auto-detection, start the query with either `#legacysql` or `#standardsql` comment line
- Legacy syntax is less predictable, especially for complex queries and may be disabled in the future
- Case classes generated by `@BigQueryType.fromTable` or `@BigQueryType.fromQuery` are not recognized in IntelliJ IDEA, but see @ref[this section](#how-to-make-intellij-idea-work-with-type-safe-bigquery-classes-) for a workaround

#### How do I work with nested Options in type safe BigQuery?

Any nullable field in BigQuery is translated to `Option[T]` by the type safe BigQuery API and it can be clunky to work with rows with multiple or nested fields. For example:

```scala
if (row.getUser.isDefined) {  // Option[User]
  val email = row.getUser.get.getEmail  // Option[String]
  if (email.isDefined) {
    doSomething(email.get)
  }
}
```

For comprehension is a nicer alternative in these cases:
```scala
val e = for (u <- row.getUser; e <- u.getUser) yield e  // Option[String]
e.foreach(doSomething)
```

Also see these [slides](http://www.lyh.me/slides/for-yield.html) and this [blog article](https://dzone.com/articles/scala-comprehensions-options).

#### How do I unit test BigQuery queries?

BigQuery doesn't provide a way to unit test query logic locally, but we can query the service directly in an integration test. Take a look at @github[BigQueryIT.scala](/scio-bigquery/src/it/scala/com/spotify/scio/bigquery/BigQueryIT.scala). `MockBigQuery` will create temporary tables on the service, feed them with mock data, and substitute table references in your query string with the mocked ones.

#### How do I stream to a partitioned BigQuery table?

Currently there is no way to create a [partitioned](https://cloud.google.com/bigquery/docs/partitioned-tables) BigQuery table via Scio/Beam when streaming, however it is possible to stream to a partitioned table if it is already created.

This can be done by using fixed windows and using the window bounds to infer date. As of Scio 0.4.0-beta2 this looks as follows:
```scala
class DayPartitionFunction() extends SerializableFunction[ValueInSingleWindow[TableRow], TableDestination] {
  override def apply(input: ValueInSingleWindow[TableRow]): TableDestination = {
    val partition = DateTimeFormat.forPattern("yyyyMMdd").withZone(DateTimeZone.UTC)
      .print(input.getWindow.asInstanceOf[IntervalWindow].start())
    new TableDestination("project:dataset.partitioned$" + partition, "")
  }
}

sc.pubsubSubscription("projects/data-university/topics/data-university")
  .withFixedWindows(30L)
// Convert to `TableRow`
  .map(myStringToTableRowConversion: String => TableRow)
  .saveAsCustomOutput(
    "SaveAsDayPartitionedBigQuery",
    BigQueryIO.writeTableRows().to(
      new DayPartitionFunction())
      .withWriteDisposition(WriteDisposition.WRITE_APPEND)
      .withCreateDisposition(CreateDisposition.CREATE_NEVER)
  )
```

In Scio 0.3.X it is possible to achieve the same behaviour using `SerializableFunction[BoundedWindow, String]` and `BigQueryIO.Write.to`. It is also possible to stream to separate tables with a Date suffix by modifying `DayPartitionFunction`, specifying the Schema, and changing the CreateDisposition to `CreateDisposition.CREATE_IF_NEEDED`.

#### How do I invalidate cached BigQuery results or disable cache?

Scio's @scaladoc[BigQuery client](com.spotify.scio.bigquery.client.BigQuery) in Scio caches query result in system property `bigquery.cache.directory`, which defaults to `$PWD/.bigquery`. Use `rm -rf .bigquery` to invalidate all cached results. To disable caching, set system property `bigquery.cache.enabled` to `false`.

#### How does BigQuery determines job priority?

By default Scio runs BigQuery jobs with `BATCH` priority except when in the REPL where it runs with `INTERACTIVE`. To override this, set system property `bigquery.priority` to either `BATCH` or `INTERACTIVE`.

### Streaming questions

#### How do I update a streaming job?

Dataflow allows streaming jobs to be updated on the fly by specifying `--update`, along with `--jobName=[your_job]` on the command line. See https://cloud.google.com/dataflow/pipelines/updating-a-pipeline for detailed docs. Note that for this to work, Dataflow needs to be able to identify which transformations from the original job map to those in the replacement job. The easiest way to do so is to give unique names to transforms in the code itself. In Scio, this can be achieved by calling `.withName()` before applying the transform. For example:

```
sc.textFile(...)
   .withName("MakeUpper").map(_.toUpperCase)
   .withName("BigWords").filter(_.length > 6)
```

In this example, the `map`'s transform name is "MakeUpper" and the `filter`'s is "BigWords". If we later decided that we want to count 6 letter words as "big" too, then we can change it to `_.length > 5`, and because the transform name is the same the job can be updated on the fly.

### Other IO components

#### How do I access various files outside of a ScioContext?

- For Scio version >= `0.4.0`

Starting from Scio `0.4.0` you can use Apache Beam's @javadoc[Filesystems](org.apache.beam.sdk.io.FileSystems) abstraction:

```scala
// the path can be any of the supported Filesystems, e.g. local, GCS, HDFS
val readmeResource = FileSystems.matchNewResource("gs://<bucket>/README.md")
val readme = FileSystems.open(readmeResource)
```

- For Scio version &lt; `0.4.0`

@@@ note
This part is GCS specific.
@@@

You can get a @javadoc[`GcsUtil`](org.apache.beam.sdk.extensions.gcp.options.GcsOptions#getGcsUtil--) instance from `ScioContext`, which can be used to open GCS files in read or write mode.

```scala
val gcsUtil = sc.optionsAs[GcsOptions].getGcsUtil
```

#### How do I reduce Datastore boilerplate?

Datastore `Entity` class is actually generated from @github[Protobuf](/scio-examples/src/test/scala/com/spotify/scio/examples/extra/ShapelessDatastoreExampleTest.scala) which uses the builder pattern and very boilerplate heavy. You can use the [shapeless-datatype](https://github.com/nevillelyh/shapeless-datatype#datastoretype) library to seamlessly convert bewteen case classes and `Entity`s. See @extref[ShapelessDatastoreExample.scala](example:ShapelessDatastoreExample) for an example job and @github[ShapelessDatastoreExampleTest.scala](/scio-examples/src/test/scala/com/spotify/scio/examples/extra/ShapelessDatastoreExampleTest.scala) for tests.

#### How do I throttle Bigtable writes?

Currently Dataflow autoscaling may not work well with large writes BigtableIO. Specifically It does not take into account Bigtable IO rate limits and may scale up more workers and end up hitting the limit and eventually fail the job. As a workaround, you can enable throttling for Bigtable writes in Scio 0.4.0-alpha2 or later.

```scala
val btOptions = new BigtableOptions.Builder()
  .setProjectId(btProjectId)
  .setInstanceId(btInstanceId)
  .setBulkOptions(new BulkOptions.Builder()
    .enableBulkMutationThrottling()
    .setBulkMutationRpcTargetMs(10) // lower latency threshold, default is 100
    .build())
  .build()
data.saveAsBigtable(btOptions, btTableId)
```

#### How do I use custom Kryo serializers?

See @ref[Kryo](internals/Kryo.md) for more.

Define a registrar class that extends `IKryoRegistrar` and annotate it with `@KryoRegistrar`. Note that the class name must ends with `KryoRegistrar`, i.e. `MyKryoRegistrar` for Scio to find it.

```scala
import com.twitter.chill._

@KryoRegistrar
class MyKryoRegistrar extends IKryoRegistrar {
  override def apply(k: Kryo): Unit = {
    // register serializers for additional classes here
    k.forClass(new UserRecordSerializer)
    k.forClass(new AccountRecordSerializer)
    ...
  }
}
```

Registering just the classes can also improve Kryo performance. By registering, classes will be serialized as numeric IDs instead of fully qualified class names, hence saving space and network IO while shuffling.

```scala
import com.twitter.chill._

@KryoRegistrar
class MyKryoRegistrar extends IKryoRegistrar {
  override def apply(k: Kryo): Unit = {
    k.registerClasses(List(classOf[MyRecord1], classOf[MyRecord2]))
  }
}
```

#### What Kryo tuning options are there?

See @github[KryoOptions.java](/scio-core/src/main/java/com/spotify/scio/options/KryoOptions.java) for a complete list of available Kryo tuning options. These can be passed via command line, for example:

`--kryoBufferSize=1024 --kryoMaxBufferSize=8192 --kryoReferenceTracking=false --kryoRegistrationRequired=true`

Among these, `--kryoRegistrationRequired=true` might be useful when developing to ensure that all data types in the pipeline are registered.

### Development environment issues

#### How do I keep SBT from running out of memory?

SBT might run out of memory sometimes and show an `OutOfMemoryError: Metaspace` error. Override default memory setting with `-mem <integer>`, e.g. `sbt -mem 1024`.

#### How do I fix SBT heap size error in IntelliJ?

If you encounter an SBT error with message "Initial heap size set to a larger value than the maximum heap size", that is because IntelliJ has a lower default `-Xmx` for SBT than `-Xms` in our `.jvmopts`. To fix that, open `Preferences` -> `Build, Execution, Deployment` -> `Build Tools` -> `sbt`, and update `Maximum heap size, MB` to `2048`.

#### How do I fix "Unable to create parent directories" error in IntelliJ?

You might get an error message like `java.io.IOException: Unable to create parent directories of /Applications/IntelliJ IDEA CE.app/Contents/bin/.bigquery/012345abcdef.schema.json`. This usually happens to people who run IntelliJ IDEA with its bundled JVM. There are two solutions.

- Install JDK from [java.com](https://www.java.com/) and switch to it by following the "All platforms: switch between installed runtimes" section in this [page](https://intellij-support.jetbrains.com/hc/en-us/articles/206544879-Selecting-the-JDK-version-the-IDE-will-run-under).
- Override the bigquery `.cache` directory as a JVM compiler parameter. On the bottom right of the IntelliJ window, click the icon that looks like a clock, and then "Configure...". Then, edit the JVM parameters to include the line `-Dbigquery.cache.directory=</path/to/repository>/.bigquery`. Then, restart the compile server by clicking on the clock icon -> Stop, and then Start.

#### How to make IntelliJ IDEA work with type safe BigQuery classes?

Due to issue [SCL-8834](https://youtrack.jetbrains.com/oauth?state=%2Fissue%2FSCL-8834) case classes generated by `@BigQueryType.fromTable` or `@BigQueryType.fromQuery` are not recognized in IntelliJ IDEA. There are two workarounds. The first, IDEA plugin solution, is highly recommended.

- IDEA Plugin

Inside IntelliJ, `Preferences` -> `Plugins` -> `Browse repositories ...` and search `Scio`. Install the plugin, restart IntelliJ, recompile the project (use SBT or [IntelliJ](https://github.com/spotify/scio-idea-plugin#bigquery-location)). You have to recompile the project each time you add/edit `@BigQueryType` macro. Plugin requires Scio >= `0.2.2`. [Documentation](https://github.com/spotify/scio-idea-plugin#scio-idea-plugin).

- Use case class from `@BigQueryType.toTable`

First start Scio REPL and generate case classes from your query or table.

```
scio> @BigQueryType.fromQuery("SELECT tornado, month FROM [publicdata:samples.gsod]") class Tornado
defined class Tornado
defined object Tornado
```

Next print Scala code of the generated classes.

```
scio> Tornado.toPrettyString()
res1: String =
@BigQueryType.toTable
case class Tornado(tornado: Option[Boolean], month: Long)
```

You can then paste the `@BigQueryType.toTable` code into your pipeline and use it with `sc.typedBigQuery`.

```scala
@BigQueryType.toTable
case class Tornado(tornado: Option[Boolean], month: Long)

def main(cmdlineArgs: Array[String]): Unit = {
  // ...
  sc.typedBigQuery[Tornado]("SELECT tornado, month FROM [publicdata:samples.gsod]")
  // ...
}
```

### Common issues

#### What does "Cannot prove that T1 <:< T2" mean?

Sometimes you get an error message like `Cannot prove that T1 <:< T2` when saving an `SCollection`. This is because some sink methods have an implicit argument like this which means element type `T` of `SCollection[T]` must be a sub-type of `TableRow` in order to save it to BigQuery. You have to map out elements to the required type before saving.

```scala
def saveAsBigQuery(tableSpec: String)(implicit ev: T <:< TableRow)
```

In the case of `saveAsTypedBigQuery` you might get an `Cannot prove that T <:< com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation.` error message. This API requires an `SCollection[T]` where `T` is a case class annotated with `@BigQueryType.toTable`. For example:

```scala
@BigQueryType.toTable
case class Result(user: String, score: Int)

p.map(kv => Result(kv._1, kv._2)).saveAsTypedBigQuery(args("output"))
```

@@@ note
Scio uses [Macro Annotations](https://docs.scala-lang.org/overviews/macros/annotations.html) and [Macro Paradise](https://docs.scala-lang.org/overviews/macros/paradise.html) plugin to implement annotations. You need to add Macro Paradise plugin to your scala compiler as described [here](https://docs.scala-lang.org/overviews/macros/paradise.html).
@@@

#### How do I fix invalid default BigQuery credentials?

If you don't specify a secret credential file for BigQuery `[1]`, Scio will use your default credentials (via [GoogleCredential.getApplicationDefault](https://github.com/google/google-auth-library-java/blob/master/oauth2_http/java/com/google/auth/oauth2/GoogleCredentials.java#L57)), which:

> Returns the Application Default Credentials which are used to identify and authorize the whole application. The following are searched (in order) to find the Application Default Credentials:
- Credentials file pointed to by the `GOOGLE_APPLICATION_CREDENTIALS` environment variable
- Credentials provided by the Google Cloud SDK
- `gcloud auth application-default login` command
- Google App Engine built-in credentials
- Google Cloud Shell built-in credentials
- Google Compute Engine built-in credentials

The easiest way to configure it on your local machine is to use the `gcloud auth application-default login` command.

`[1]` Keep in mind that you can specify your credential file via `-Dbigquery.secret`.

#### Why are my typed BigQuery case classes not up to date?

Case classes generated by `@BigQueryType.fromTable` or other macros might not update after table schema change. To solve this problem, remove the cached BigQuery metadata by deleting the `.bigquery` directory in your project root. If you would rather avoid any issues resulting from caching and schema evolution entirely, you can disable caching by setting the system property `bigquery.cache.enabled` to `false`.

#### How do I fix "SocketTimeoutException" with BigQuery?

BigQuery requests may sometimes timeout, i.e. for complex queries over many tables.
```
exception during macro expansion:
[error] java.net.SocketTimeoutException: Read timed out
```

It can be fixed by increasing the timeout settings (default 20s).
```bash
sbt -Dbigquery.connect_timeout=30000 -Dbigquery.read_timeout=30000
```

#### Why do I see names like "main@{NativeMethodAccessorImpl...}" in the UI?

Scio traverses JVM stack trace to figure out the proper name of each transform, i.e. `flatMap@{UserAnalysis.scala:30}` but may get confused if your jobs are under the `com.spotify.scio` package. Move them to a different package, e.g. `com.spotify.analytics` to fix the issue.

#### How do I fix "RESOURCE_EXHAUSTED" error?

You might see errors like `RESOURCE_EXHAUSTED: IO error: No space left on disk` in a job. They usually indicate that you have allocated insufficient local disk space to process your job. If you are running your job with default settings, your job is running on 3 workers, each with 250 GB of local disk space. Consider modifying the default settings to increase the number of workers available to your job (via `--numWorkers`), to increase the default disk size per worker (via `--diskSizeGb`).

#### Can I use "scala.App" trait instead of "main" method?

Your Scio applications should define a `main` method instead of extending `scala.App`. Applications extending `scala.App` due to delayed initialization and closure cleaning may not work properly.

#### How to inspect the content of an `SCollection`?

There is multiple options here:
- Use `debug()` method on an `SCollection` to print its content as the data flows through the DAG during the execution (after the `close` or `closeAndCollect`)
- Use a debugger and setup break points - make sure to break inside of your functions to stop control at the execution not the pipeline construction time
- In [[Scio-REPL]], use `closeAndCollect()` to close the context and materialize the content of an `SCollection`

#### How do I improve side input performance?

By default Dataflow workers allocate 100MB (see @javadoc[DataflowWorkerHarnessOptions#getWorkerCacheMb](org.apache.beam.runners.dataflow.options.DataflowWorkerHarnessOptions#getWorkerCacheMb--)) of memory for caching side inputs, and falls back to disk or network. Therefore jobs with large side inputs may be slow. To override this default, register `DataflowWorkerHarnessOptions` before parsing command line arguments and then pass `--workerCacheMb=N` when submitting the job.

```scala
PipelineOptionsFactory.register(classOf[DataflowWorkerHarnessOptions])
val (sc, args) = ContextAndArgs(cmdlineArgs)
```

#### How do I control concurrency (number of DoFn threads) in Dataflow workers

By default Google Cloud Dataflow will use as many threads (concurrent DoFns) per worker as appropriate (precise definition is an implementation detail), in same cases you might want to control this. Use `NumberOfWorkerHarnessThreads` option from `DataflowPipelineDebugOptions`. For example to use a single thread per worker on 8 vCPU machine, simply specify 8 vCPU worker machine type, and `--numberOfWorkerHarnessThreads=1` in CLI or set corresponding option in `DataflowPipelineDebugOptions`.

#### How to manually investigate a Cloud Dataflow worker

First find the VM of the worker, the easiest place is through the GCE instance groups:

```
gcloud compute ssh --project=<project> --zone=<zone> <VM>
```

To find the id of batch (for `batch` job) container:

```
docker ps | grep "batch\|streaming" | awk '{print $1}'
```

To get into the harness container:

```
docker exec -it <container-id> /bin/bash
```

To install java jdk tools:

```
apt-get update
apt-get install default-jdk -y
```

To find java process:

```
jps
```

To get GC stats:

```
jstat -gcutil <pid> 1000 1000
```

To get stacktrace:

```
jstack <pid>
```
