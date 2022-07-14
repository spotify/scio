package com.spotify.scio.parquet.read

object ParquetReadConfiguration {

  // Key
  val SplitGranularity = "scio.parquet.read.splitgranularity"

  // Values
  val SplitGranularityFile = "file"
  val SplitGranularityRowGroup = "rowgroup"
}
