/*
 * Copyright 2016 Spotify AB.
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

import com.google.api.client.util.{BackOffUtils, Sleeper, BackOff}
import com.google.api.services.bigquery.model.TableReference
import com.google.cloud.dataflow.sdk.io.BigQueryIO
import com.spotify.scio.bigquery.{BigQueryClient, BigQueryUtil, TableRow}
import org.apache.avro.Schema
import org.slf4j.{LoggerFactory, Logger}

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.Try

/** Exception for when a tap is not available. */
class TapNotAvailableException(msg: String) extends Exception(msg)

/** Utility for managing `Future[Tap[T]]`s. */
trait Taps {

  /** Get a `Future[Tap[T]]` for an Avro file. */
  def avroFile[T: ClassTag](path: String, schema: Schema = null): Future[Tap[T]] =
    mkTap(s"Avro: $path", () => isPathDone(path), () => AvroTap[T](path, schema))

  /** Get a `Future[Tap[T]]` for BigQuery SELECT query. */
  def bigQuerySelect(sqlQuery: String): Future[Tap[TableRow]] =
    mkTap(
      s"BigQuery SELECT: $sqlQuery", () => isQueryDone(sqlQuery), () => bigQueryTap(sqlQuery))

  /** Get a `Future[Tap[T]]` for BigQuery table. */
  def bigQueryTable(table: TableReference): Future[Tap[TableRow]] =
    mkTap(s"BigQuery Table: $table", () => tableExists(table), () => BigQueryTap(table))

  /** Get a `Future[Tap[T]]` for BigQuery table. */
  def bigQueryTable(tableSpec: String): Future[Tap[TableRow]] =
    bigQueryTable(BigQueryIO.parseTableSpec(tableSpec))

  /** Get a `Future[Tap[T]]` of TableRow for a JSON file. */
  def tableRowJsonFile(path: String): Future[Tap[TableRow]] =
    mkTap(s"TableRowJson: $path", () => isPathDone(path), () => TableRowJsonTap(path))

  /** Get a `Future[Tap[T]]` for a text file. */
  def textFile(path: String): Future[Tap[String]] =
    mkTap(s"Text: $path", () => isPathDone(path), () => TextTap(path))

  private def isPathDone(path: String): Boolean = FileStorage(path).isDone

  private def isQueryDone(sqlQuery: String): Boolean =
    BigQueryClient.defaultInstance().getQueryTables(sqlQuery).forall(tableExists)

  private def tableExists(table: TableReference): Boolean =
    Try(BigQueryClient.defaultInstance().getTableSchema(table)).isSuccess

  private def bigQueryTap(sqlQuery: String): BigQueryTap = {
    val bq = BigQueryClient.defaultInstance()
    val table = bq.query(sqlQuery)
    BigQueryTap(table)
  }

  /**
   * Make a tap, to be implemented by concrete classes.
   *
   * @param name unique name of the tap
   * @param readyFn function to check if the tap is ready
   * @param tapFn function to create the tap
   */
  private[scio] def mkTap[T](name: String,
                             readyFn: () => Boolean,
                             tapFn: () => Tap[T]): Future[Tap[T]]

}

/** Taps implementation that fails immediately if tap not available. */
private class ImmediateTaps extends Taps {
  override private[scio] def mkTap[T](name: String,
                                      readyFn: () => Boolean,
                                      tapFn: () => Tap[T]): Future[Tap[T]] =
    if (readyFn()) Future.successful(tapFn()) else Future.failed(new TapNotAvailableException(name))
}

/** Taps implementation that polls for tap availability in the background. */
private class PollingTaps(private val backOff: BackOff) extends Taps {

  case class Poll(name: String,
                  readyFn: () => Boolean,
                  tapFn: () => Tap[Any],
                  promise: Promise[AnyRef])

  private var polls: List[Poll] = null
  private val logger: Logger = LoggerFactory.getLogger(classOf[PollingTaps])

  override private[scio] def mkTap[T](name: String,
                                      readyFn: () => Boolean,
                                      tapFn: () => Tap[T]): Future[Tap[T]] = this.synchronized {
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
            logger.info(s"Polling for ${polls.size} tap" + (if (polls.size > 1) "s" else ""))
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
      }
    }

    p.future.asInstanceOf[Future[Tap[T]]]
  }

}

/** Companion object for [[Taps]]. */
object Taps extends {

  /** System property key for taps algorithm. */
  val ALGORITHM_KEY = "taps.algorithm"

  /** Default taps algorithm. */
  val ALGORITHM_DEFAULT = "immediate"

  /** System property key for polling taps maximum interval in milliseconds. */
  val POLLING_MAXIMUM_INTERVAL_KEY = "taps.polling.maximum_interval"

  /** Default polling taps maximum interval. */
  val POLLING_MAXIMUM_INTERVAL_DEFAULT = "600000"

  /** System property key for polling taps initial interval in milliseconds. */
  val POLLING_INITIAL_INTERVAL_KEY = "taps.polling.initial_interval"

  /** Default polling taps initial interval. */
  val POLLING_INITIAL_INTERVAL_DEFAULT = "10000"

  /**
   * System property key for polling taps maximum number of attempts, unlimited if <= 0. Default is
   * 0.
   */
  val POLLING_MAXIMUM_ATTEMPTS_KEY = "taps.polling.maximum_attempts"

  /** Default polling taps maximum number of attempts. */
  val POLLING_MAXIMUM_ATTEMPTS_DEFAULT = "0"

  /**
   * Create a new Taps instance.
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
  def apply(): Taps = {
    import com.google.cloud.dataflow.sdk.util
    getPropOrElse(ALGORITHM_KEY, ALGORITHM_DEFAULT) match {
      case "immediate" => new ImmediateTaps
      case "polling" =>
        val maxAttempts =
          getPropOrElse(POLLING_MAXIMUM_ATTEMPTS_KEY, POLLING_MAXIMUM_ATTEMPTS_DEFAULT).toInt
        val initInterval =
          getPropOrElse(POLLING_INITIAL_INTERVAL_KEY, POLLING_INITIAL_INTERVAL_DEFAULT).toLong
        val backOff = if (maxAttempts <= 0) {
          val maxInterval =
            getPropOrElse(POLLING_MAXIMUM_INTERVAL_KEY, POLLING_MAXIMUM_INTERVAL_DEFAULT).toLong
          new util.IntervalBoundedExponentialBackOff(maxInterval, initInterval)
        } else {
          new util.AttemptBoundedExponentialBackOff(maxAttempts, initInterval)
        }
        new PollingTaps(backOff)
      case t => throw new IllegalArgumentException(s"Unsupported Taps $t")
    }
  }

  private def getPropOrElse(key: String, default: String): String = {
    val value = sys.props(key)
    if (value == null) default else value
  }

}
