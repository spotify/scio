package com.spotify.scio.examples.extra

import com.google.cloud.dataflow.sdk.testing.DataflowAssert
import com.spotify.scio._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object TapExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    // first job
    val (sc1, args) = ContextAndArgs(cmdlineArgs)
    val f1 = sc1.parallelize(1 to 10)
      .sum
      .materialize  // save data to a temporary location for use later
    val f2 = sc1.parallelize(1 to 100)
      .sum
      .map(_.toString)
      .saveAsTextFile(args("output"))  // save data for use later
    sc1.close()

    // wait for future completion in case job is non-blocking
    val t1 = Await.result(f1, Duration.Inf)
    val t2 = Await.result(f2, Duration.Inf)

    // fetch tap values directly
    println(t1.value.mkString(", "))
    println(t2.value.mkString(", "))

    // second job
    val (sc2, _) = ContextAndArgs(cmdlineArgs)
    // re-open taps in new context
    val s = (t1.open(sc2) ++ t2.open(sc2).map(_.toInt)).sum
    DataflowAssert.thatSingleton(s.internal).isEqualTo((1 to 10).sum + (1 to 100).sum)
    val result = sc2.close()

    // block main() until second job completes
    println(Await.result(result.finalState, Duration.Inf))
  }
}
