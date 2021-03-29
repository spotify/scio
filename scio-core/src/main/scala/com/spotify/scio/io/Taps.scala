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

package com.spotify.scio.io

import com.spotify.scio.{registerSysProps, SysProp}
import org.apache.beam.sdk.util.{BackOff, BackOffUtils, FluentBackoff, Sleeper}
import org.joda.time.Duration
import org.slf4j.LoggerFactory

import scala.concurrent.{Future, Promise}

/** Exception for when a tap is not available. */
class TapNotAvailableException(msg: String) extends Exception(msg)

/** Utility for managing `Future[Tap[T]]`s. */
trait Taps {

  /** Get a `Future[Tap[String]]` for a text file. */
  def textFile(path: String): Future[Tap[String]] =
    mkTap(s"Text: $path", () => isPathDone(path), () => TextTap(path))

  private[scio] def isPathDone(path: String): Boolean = FileStorage(path).isDone

  /**
   * Make a tap, to be implemented by concrete classes.
   *
   * @param name unique name of the tap
   * @param readyFn function to check if the tap is ready
   * @param tapFn function to create the tap
   */
  private[scio] def mkTap[T](
    name: String,
    readyFn: () => Boolean,
    tapFn: () => Tap[T]
  ): Future[Tap[T]]
}

/** Taps implementation that fails immediately if tap not available. */
final private class ImmediateTaps extends Taps {
  override private[scio] def mkTap[T](
    name: String,
    readyFn: () => Boolean,
    tapFn: () => Tap[T]
  ): Future[Tap[T]] =
    if (readyFn()) Future.successful(tapFn())
    else Future.failed(new TapNotAvailableException(name))
}

private object PollingTaps {
  private val logger = LoggerFactory.getLogger(this.getClass)

  final case class Poll(
    name: String,
    readyFn: () => Boolean,
    tapFn: () => Tap[Any],
    promise: Promise[AnyRef]
  )
}

/** Taps implementation that polls for tap availability in the background. */
final private class PollingTaps(private[this] val backOff: BackOff) extends Taps {
  import PollingTaps._

  private[this] var polls: List[Poll] = _

  override private[scio] def mkTap[T](
    name: String,
    readyFn: () => Boolean,
    tapFn: () => Tap[T]
  ): Future[Tap[T]] =
    this.synchronized {
      val p = Promise[AnyRef]()
      val init = if (polls == null) {
        polls = Nil
        true
      } else {
        false
      }

      logger.info(s"Polling for tap $name")
      polls +:= Poll(name, readyFn, tapFn.asInstanceOf[() => Tap[Any]], p)

      if (init) {
        import scala.concurrent.ExecutionContext.Implicits.global
        Future {
          val sleeper = Sleeper.DEFAULT
          do {
            if (polls.nonEmpty) {
              val tap = if (polls.size > 1) "taps" else "tap"
              logger.info(s"Polling for ${polls.size} $tap")
            }
            this.synchronized {
              val (ready, pending) = polls.partition(_.readyFn())
              ready.foreach { p =>
                logger.info(s"Tap available: ${p.name}")
                p.promise.success(tapFn())
              }
              polls = pending
            }
          } while (BackOffUtils.next(sleeper, backOff))
          polls.foreach(p => p.promise.failure(new TapNotAvailableException(p.name)))
        }
      }

      p.future.asInstanceOf[Future[Tap[T]]]
    }
}

/** Companion object for [[Taps]]. */
object Taps {
  import TapsSysProps._

  /** Default taps algorithm. */
  val AlgorithmDefault = "immediate"

  /** Default polling taps maximum interval. */
  val PollingMaximumIntervalDefault = "600000"

  /** Default polling taps initial interval. */
  val PollingInitialIntervalDefault = "10000"

  /** Default polling taps maximum number of attempts. */
  val PollingMaximumAttemptsDefault = "0"

  /**
   * Create a new [[Taps]] instance.
   *
   * Taps algorithm can be set via the `taps.algorithm` property.
   * Available algorithms are `immediate` (default) and `polling`.
   *
   * Additional properties can be set for the `polling` algorithm.
   *
   * - `taps.polling.maximum_interval`: maximum interval between polls.
   *
   * - `taps.polling.initial_interval`: initial interval between polls.
   *
   * - `taps.polling.maximum_attempts`: maximum number of attempts, unlimited if <= 0. Default is 0.
   */
  def apply(): Taps =
    Algorithm.value(AlgorithmDefault) match {
      case "immediate" => new ImmediateTaps
      case "polling" =>
        val maxAttempts =
          PollingMaximumAttempts.value(PollingMaximumAttemptsDefault).toInt
        val initInterval =
          PollingInitialInterval.value(PollingInitialIntervalDefault).toLong
        val backOff = if (maxAttempts <= 0) {
          val maxInterval =
            PollingMaximumInterval.value(PollingMaximumIntervalDefault).toLong
          FluentBackoff.DEFAULT
            .withInitialBackoff(Duration.millis(initInterval))
            .withMaxBackoff(Duration.millis(maxInterval))
            .backoff()
        } else {
          FluentBackoff.DEFAULT
            .withInitialBackoff(Duration.millis(initInterval))
            .withMaxRetries(maxAttempts)
            .backoff()
        }
        new PollingTaps(backOff)
      case t => throw new IllegalArgumentException(s"Unsupported Taps $t")
    }
}

@registerSysProps
object TapsSysProps {
  val Algorithm: SysProp = SysProp("taps.algorithm", "System property key for taps algorithm")
  val PollingMaximumInterval: SysProp = SysProp(
    "taps.polling.maximum_interval",
    "System property key for polling taps maximum interval in milliseconds"
  )
  val PollingInitialInterval: SysProp = SysProp(
    "taps.polling.initial_interval",
    "System property key for polling taps initial interval in milliseconds"
  )
  val PollingMaximumAttempts: SysProp = SysProp(
    "taps.polling.maximum_attempts",
    "System property key for polling taps maximum number of attempts, unlimited if <= 0. " +
      "Default is 0"
  )
}
