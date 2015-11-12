package com.spotify.scio.bigtable

import com.google.cloud.bigtable.dataflow.CloudBigtableOptions
import com.spotify.scio.ScioContext

/** Utilities for BigTable. */
object BigTable {

  /** Parse CloudBigtableOptions from command line arguments. */
  def parseOptions(args: Array[String]): CloudBigtableOptions =
    ScioContext.parseArguments[CloudBigtableOptions](args)._1

}
