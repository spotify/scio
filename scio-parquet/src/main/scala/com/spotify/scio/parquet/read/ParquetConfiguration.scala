package com.spotify.scio.parquet.read

object ParquetConfiguration {

  // Key
  val ParquetReadSplitGranularity = "scio.parquet.read.splitgranularity"

  // Values
  val ReadGranularityFile = "file"
  val ReadGranularityRowGroup = "rowgroup"
}
