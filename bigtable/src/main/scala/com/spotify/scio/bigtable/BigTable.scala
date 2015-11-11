package com.spotify.scio.bigtable

import com.google.cloud.bigtable.dataflow.CloudBigtableOptions
import com.spotify.scio.ScioContext

/** Utilities for BigTable. */
object BigTable {

  /** Extract CloudBigtableOptions from command line arguments. */
  def extractOptions(args: Array[String]): CloudBigtableOptions =
    ScioContext.extractOptions[CloudBigtableOptions](args)._1

}
