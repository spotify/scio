package com.spotify.scio.examples.extra

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/*
SBT
runMain
  com.spotify.scio.examples.extra.WordCountOrchestration
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --output=gs://[BUCKET]/[PATH]/wordcount
*/

object WordCountOrchestration {

  type FT[T] = Future[Tap[T]]

  def main(cmdlineArgs: Array[String]) = {
    val (opts, args) = ScioContext.parseArguments[DataflowPipelineOptions](cmdlineArgs)
    opts.setRunner(classOf[DataflowPipelineRunner])

    val output = args("output")

    // Submit count job 1
    val f1 = count(opts, ExampleData.KING_LEAR)

    //Submit count job 2
    val f2 = count(opts, ExampleData.OTHELLO)

    import scala.concurrent.ExecutionContext.Implicits.global

    // Join futures and submit merge job
    val f = Future.sequence(Seq(f1, f2)).flatMap(merge(opts, _, output))

    // Block process and wait for last future
    val t = Await.result(f, Duration.Inf)
    println("Tap:")
    t.value.take(10).foreach(println)
  }

  def count(opts: DataflowPipelineOptions, inputPath: String): FT[(String, Long)] = {
    val sc = ScioContext(opts)
    val f = sc.textFile(inputPath)
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue()
      .materialize
    sc.close()
    f
  }

  def merge(opts: DataflowPipelineOptions, s: Seq[Tap[(String, Long)]], outputPath: String): FT[String] = {
    val sc = ScioContext(opts)
    val f = SCollection.unionAll(s.map(_.open(sc)))
      .sumByKey
      .map(kv => kv._1 + " " + kv._2)
      .saveAsTextFile(outputPath)
    sc.close()
    f
  }

}
