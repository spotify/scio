package com.spotify.cloud

import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write

/**
 * Main package for public APIs. Import all.
 *
 * {{{
 * import com.spotify.cloud.dataflow._
 * }}}
 */
package object dataflow extends Implicits {

  /** Alias for [[com.spotify.cloud.dataflow.values.WindowedValue]]. */
  type WindowedValue[T] = values.WindowedValue[T]

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

}
