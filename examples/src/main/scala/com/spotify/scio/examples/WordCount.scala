package com.spotify.scio.examples

import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import com.spotify.scio._
import com.spotify.scio.examples.common.ExampleData

/*
SBT
runMain
  com.spotify.scio.examples.WordCount
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
  --output=gs://[BUCKET]/[PATH]/wordcount
*/

object WordCount {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val input = args.getOrElse("input", ExampleData.KING_LEAR)
    val output = args.optional("output").getOrElse(
      if (sc.options.getStagingLocation != null) {
        GcsPath.fromUri(sc.options.getStagingLocation).resolve("counts.txt").toString
      } else {
        throw new IllegalArgumentException("Must specify --output or --stagingLocation")
      })

    // initialize accumulators
    val max = sc.maxAccumulator[Int]("maxLineLength")
    val min = sc.minAccumulator[Int]("minLineLength")
    val sum = sc.sumAccumulator[Long]("emptyLines")

    sc.textFile(input)
      .withAccumulator(max, min, sum)  // convert to SCollectionWithAccumulator
      // specialized version of filter with AccumulatorContext as second argument
      .filter { (l, c) =>
        val t = l.trim

        // update accumulators "max" and "min"
        c.addValue(max, t.length).addValue(min, t.length)

        val b = t.isEmpty
        if (b) c.addValue(sum, 1L) // update accumulator "sum"
        !b
      }
      .toSCollection  // convert back to normal SCollection
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue()
      .map(t => t._1 + ": " + t._2)
      .saveAsTextFile(output)

    val result = sc.close()

    // wait for pipeline to complete
    while (!result.isCompleted) {
      Thread.sleep(1000)
    }

    // retrieve accumulator values
    println("Max: " + result.accumulatorTotalValue(max))
    println("Min: " + result.accumulatorTotalValue(min))
    println("Sum: " + result.accumulatorTotalValue(sum))
  }
}
