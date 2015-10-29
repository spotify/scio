package com.spotify.scio.examples.extra

import com.spotify.scio._

object AccumulatorExample {

  def main(cmdlineArgs: Array[String]): Unit = {
    val sc = ScioContext()

    val max = sc.maxAccumulator[Int]("max")
    val min = sc.minAccumulator[Int]("min")
    val sum = sc.sumAccumulator[Int]("sum")
    val count = sc.sumAccumulator[Int]("count")

    sc.parallelize(1 to 100)
      .withAccumulator(max, min, sum, count)
      .filter { (i, c) =>
        c.addValue(max, i).addValue(min, i).addValue(sum, i).addValue(count, 1)
        i <= 50
      }
      .map { (i, c) =>
        // reuse some accumulators here
        c.addValue(sum, i).addValue(count, 1)
        i
      }

    val r = sc.close()

    println("Max: " + r.accumulatorTotalValue(max))
    println("Min: " + r.accumulatorTotalValue(min))
    println("Sum: " + r.accumulatorTotalValue(sum))
    println("Count: " + r.accumulatorTotalValue(count))

    println("Sum per step:")
    r.accumulatorValuesAtSteps(sum).foreach(kv => println(kv._2 + " @ " + kv._1))

    println("Count per step:")
    r.accumulatorValuesAtSteps(count).foreach(kv => println(kv._2 + " @ " + kv._1))
  }

}
