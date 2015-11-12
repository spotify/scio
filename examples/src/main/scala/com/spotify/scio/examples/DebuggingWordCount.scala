package com.spotify.scio.examples

import com.google.cloud.dataflow.sdk.testing.DataflowAssert
import com.spotify.scio._

import scala.collection.JavaConverters._

/*
SBT
runMain
  com.spotify.scio.examples.DebuggingWordCount
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/dataflow/staging
  --input=gs://dataflow-samples/shakespeare/kinglear.txt
*/

object DebuggingWordCount {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val matchedWords = sc.sumAccumulator[Long]("matchedWords")
    val unmatchedWords = sc.sumAccumulator[Long]("unmatchedWords")

    val filteredWords = sc.textFile(args.getOrElse("input", "gs://dataflow-samples/shakespeare/kinglear.txt"))
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue()
      .withAccumulator(matchedWords, unmatchedWords)
      .filter { (kv, c) =>
        val b = Set("Flourish", "stomach").contains(kv._1)
        c.addValue(if (b) matchedWords else unmatchedWords, 1L)
        b
      }
      .toSCollection

    // verify internal PCollection
    DataflowAssert.that(filteredWords.internal)
      .containsInAnyOrder(List(("Flourish", 3L), ("stomach", 1L)).asJava)

    val result = sc.close()

    // wait for pipeline to complete
    while (!result.isCompleted) {
      Thread.sleep(1000)
    }

    // retrieve accumulator values
    require(result.accumulatorTotalValue(matchedWords) == 2)
    require(result.accumulatorTotalValue(unmatchedWords) > 100)
  }

}
