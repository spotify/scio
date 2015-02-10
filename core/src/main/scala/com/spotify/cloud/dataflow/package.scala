package com.spotify.cloud

import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write

package object dataflow extends Implicits {

  type WindowedValue[T] = values.WindowedValue[T]

  // BigQuery
  val CREATE_IF_NEEDED = Write.CreateDisposition.CREATE_IF_NEEDED
  val CREATE_NEVER = Write.CreateDisposition.CREATE_NEVER
  val WRITE_APPEND = Write.WriteDisposition.WRITE_APPEND
  val WRITE_EMPTY = Write.WriteDisposition.WRITE_EMPTY
  val WRITE_TRUNCATE = Write.WriteDisposition.WRITE_TRUNCATE

}
