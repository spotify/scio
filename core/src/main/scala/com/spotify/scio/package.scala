package com.spotify

import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write
import com.google.cloud.dataflow.sdk.util.WindowingStrategy.AccumulationMode
import com.spotify.scio.values.{
  AccumulatorType, DoubleAccumulatorType, LongAccumulatorType, IntAccumulatorType
}

/**
 * Main package for public APIs. Import all.
 *
 * {{{
 * import com.spotify.scio._
 * }}}
 */
package object scio {

  /** Alias for BigQuery CreateDisposition. */
  val CREATE_IF_NEEDED = Write.CreateDisposition.CREATE_IF_NEEDED

  /** Alias for BigQuery CreateDisposition. */
  val CREATE_NEVER = Write.CreateDisposition.CREATE_NEVER

  /** Alias for BigQuery WriteDisposition. */
  val WRITE_APPEND = Write.WriteDisposition.WRITE_APPEND

  /** Alias for BigQuery WriteDisposition. */
  val WRITE_EMPTY = Write.WriteDisposition.WRITE_EMPTY

  /** Alias for BigQuery WriteDisposition. */
  val WRITE_TRUNCATE = Write.WriteDisposition.WRITE_TRUNCATE

  /** Alias for WindowingStrategy AccumulationMode.ACCUMULATING_FIRED_PANES. */
  val ACCUMULATING_FIRED_PANES = AccumulationMode.ACCUMULATING_FIRED_PANES

  /** Alias for WindowingStrategy AccumulationMode.DISCARDING_FIRED_PANES. */
  val DISCARDING_FIRED_PANES = AccumulationMode.DISCARDING_FIRED_PANES

  implicit val intAccumulatorType: AccumulatorType[Int] = new IntAccumulatorType
  implicit val longAccumulatorType: AccumulatorType[Long] = new LongAccumulatorType
  implicit val doubleAccumulatorType: AccumulatorType[Double] = new DoubleAccumulatorType

}
