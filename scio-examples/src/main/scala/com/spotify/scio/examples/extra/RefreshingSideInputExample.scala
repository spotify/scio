/*
 * Copyright 2019 Spotify AB.
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

import com.spotify.scio._
import com.spotify.scio.pubsub._
import com.spotify.scio.values.WindowOptions
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.options.StreamingOptions
import org.apache.beam.sdk.transforms.windowing.Window.ClosingBehavior
import org.apache.beam.sdk.transforms.windowing.{AfterPane, Repeatedly}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.{Duration, Instant}
import org.slf4j.LoggerFactory

import scala.util.{Random, Success, Try}

/**
 * Streaming job with periodically updating side input, modeled as a basic lottery game. A side
 * input holds a sequence of randomly generated numbers that are the current "winning" numbers, and
 * refreshes every 10 seconds. Meanwhile, a Pub/Sub subscription reads in lottery tickets
 * (represented as Strings) and checks if they match the winning numbers.
 */
object RefreshingSideInputExample {
  case class LotteryTicket(numbers: Seq[Int])
  case class LotteryResult(
    eventTime: Instant,
    processTime: Instant,
    isWinner: Boolean,
    ticket: Seq[Int],
    winningNumbers: Seq[Int]
  )

  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  private val ticketSize = 5

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.optionsAs[StreamingOptions].setStreaming(true)

    // An unbounded input that produces a sequence of 5 randomly generated winning lottery numbers,
    // refreshed every 10 seconds. Materialized as a singleton `SideInput`.
    val winningLotteryNumbers = sc
      .customInput(
        "winningLotteryNumbers",
        GenerateSequence
          .from(0)
          .withRate(1, Duration.standardSeconds(10))
      )
      .withFixedWindows(
        duration = Duration.standardSeconds(10),
        offset = Duration.ZERO,
        options = WindowOptions(
          trigger = Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
          accumulationMode = AccumulationMode.DISCARDING_FIRED_PANES,
          closingBehavior = ClosingBehavior.FIRE_IF_NON_EMPTY,
          allowedLateness = Duration.standardSeconds(0)
        )
      )
      .map(_ => Seq.fill(ticketSize)(Random.nextInt(100)))
      // A default is needed in case an empty pane is fired
      .asSingletonSideInput(Seq.fill(ticketSize)(-1))

    // Sample PubSub topic modeling lottery tickets as a comma-separated list of numbers.
    // For example, a message might contain the string "10,7,3,1,9"
    sc.pubsubTopic[String](args("input"))
      .flatMap(toLotteryTicket)
      .withFixedWindows(Duration.standardSeconds(5))
      .withTimestamp
      .withSideInputs(winningLotteryNumbers)
      .map { case ((lotteryTicket, eventTime), side) =>
        val currentWinningNumbers = side(winningLotteryNumbers)

        val isWinner = lotteryTicket.numbers == currentWinningNumbers
        val result = LotteryResult(
          eventTime,
          Instant.now(),
          isWinner,
          lotteryTicket.numbers,
          currentWinningNumbers
        )

        logger.info(s"Lottery result: $result")
      } // Can save output to PubSub, BigQuery, etc.

    sc.run()
    ()
  }

  private def toLotteryTicket(message: String): Option[LotteryTicket] =
    Try(LotteryTicket(message.split(",").map(_.toInt).toSeq)) match {
      case Success(s) if s.numbers.size == ticketSize => Some(s)
      case _ =>
        logger.error(s"Malformed message: $message")
        None
    }
}
