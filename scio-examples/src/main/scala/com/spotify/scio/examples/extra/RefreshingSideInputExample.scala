/*
 * Copyright 2018 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// Example: Demonstrates a streaming job with periodically refreshing side input
// Usage:

// `sbt "scio-examples/runMain com.spotify.scio.examples.extra.RefreshingSideInputExample
// --project=[PROJECT] --runner=[RUNNER] --zone=[ZONE] --input=[PUBSUB_SUBSCRIPTION]"`
package com.spotify.scio.examples.extra

import com.spotify.scio.ScioContext
import com.spotify.scio.values.WindowOptions
import org.apache.beam.examples.common.ExampleOptions
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior
import org.apache.beam.sdk.transforms.windowing.{AfterPane, Repeatedly}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.{Duration, Instant}
import org.slf4j.LoggerFactory

import scala.util.{Random, Try}

/**
 * Streaming job with periodically updating side input, modeled as a basic lottery game.
 * A side input holds a sequence of randomly generated numbers that are the current "winning"
 * numbers, and refreshes every 10 seconds. Meanwhile, a pubsub subscription reads in lottery
 * tickets (represented as Strings) and checks if they contain any winning numbers.
 */
object RefreshingSideInputExample {
  case class LotteryTicket(numbers: Seq[Int])
  case class LotteryResult(
                            ticketCreatedTimestamp: Instant,
                            windowProcessedTimestamp: Instant,
                            isWinner: Boolean,
                            ticket: Seq[Int],
                            winningNumbers: Seq[Int]
                          )

  private lazy val logger = LoggerFactory.getLogger(RefreshingSideInputExample.getClass)
  private lazy val random = new Random()

  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[ExampleOptions](cmdlineArgs)
    opts.as(classOf[StreamingOptions]).setStreaming(true)
    val sc = ScioContext(opts)

    // An unbounded input that produces 5 randomly generated winning lottery numbers,
    // refreshed every 10 seconds. Materialized as a ListSideInput.
    val winningLotteryNumbers = sc
      .customInput(
        "winningNumbers",
        GenerateSequence
          .from(0)
          .withRate(1, Duration.standardSeconds(1))
      )
      .withFixedWindows(
        duration = Duration.standardSeconds(10),
        offset = Duration.ZERO,
        options = WindowOptions(
          trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(5)),
          accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
          closingBehavior = ClosingBehavior.FIRE_IF_NON_EMPTY,
          allowedLateness = Duration.standardSeconds(5))
      )
      .map(_ => random.nextInt(100))
      .asListSideInput

    // Sample pubsub topic modeling lottery tickets as a comma-separated list of numbers.
    // For example, a message might contain the string "10,7,3"
    sc.pubsubSubscription[String](args("input"))
      .flatMap(toLotteryTicket)
      .timestampBy(_ => Instant.now())
      .withTimestamp
      .withFixedWindows(Duration.standardSeconds(10))
      .withSideInputs(winningLotteryNumbers)
      .map { case ((lotteryTicket, ticketCreatedTimestamp), side) =>
        val currentWinningNumbers = side(winningLotteryNumbers)

        val isWinner = lotteryTicket.numbers.intersect(currentWinningNumbers).nonEmpty
        val result = LotteryResult(ticketCreatedTimestamp, Instant.now(), isWinner,
          lotteryTicket.numbers, currentWinningNumbers)

        logger.info(s"Lottery Result: $result")
      } // can send output to pub/sub, save to bigquery, etc

    sc.close()
  }

  private def toLotteryTicket(message: String): Option[LotteryTicket] = {
    Try(LotteryTicket(message.split(",").map(_.toInt))).toOption
  }
}
