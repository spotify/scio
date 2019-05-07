# Runners

## Runner dependency

Starting Scio 0.4.4, Beam runner is completely decoupled from `scio-core`, which no longer depend on any Beam runner now. Add runner dependencies to enable execution on specific backends. For example, when using Scio 0.4.7 which depends on Beam 2.2.0, you should add the following dependencies to run pipelines locally and on Google Cloud Dataflow.

```scala
libraryDependencies ++= Seq(
  "org.apache.beam" % "beam-runners-direct-java" % "2.2.0",
  "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % "2.2.0"
)
```

## Runner specific logic

Dataflow specific logic, e.g. job ID, metrics, were also removed from `ScioResult`. You can convert between the generic `ScioResult` and runner specific result types like the example below. Note that currently only `DataflowResult` is implemented.

```scala mdoc:silent
import com.spotify.scio.{ScioContext, ClosedScioContext, ScioResult}

object SuperAwesomeJob {
  def main(cmdlineArgs: Array[String]): Unit = {

    val sc: ScioContext = ???

    // Job code
    // ...

    // Generic result only
    val closedContext: ClosedScioContext = sc.close()
    val scioResult: ScioResult = closedContext.waitUntilFinish()

    // Convert to Dataflow specific result
    import com.spotify.scio.runners.dataflow.DataflowResult
    val dfResult: DataflowResult = scioResult.as[DataflowResult]

    // Convert back to generic result
    val scioResult2: ScioResult = dfResult.asScioResult

    ()
  }
}

```

Given the Google Cloud project ID and Dataflow job ID, one can also create `DataflowResult` and `ScioResult` without running a pipeline. This could be when submitting jobs asynchronously and retrieving metrics later.

```scala mdoc:reset
import com.spotify.scio.runners.dataflow.DataflowResult

object AnotherAwesomeJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val dfResult = DataflowResult("<PROJECT_ID>", "<REGION>", "<JOB_ID>")
    val scioResult = dfResult.asScioResult
    // Some code
  }
}
```
