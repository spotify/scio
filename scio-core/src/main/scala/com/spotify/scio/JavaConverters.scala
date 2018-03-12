package com.spotify.scio

import org.apache.beam.sdk.io.{DefaultFilenamePolicy, FileBasedSink}
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider

object JavaConverters {

  implicit def toResourceId(filenamePrefix: String): ResourceId =
    FileBasedSink.convertToFileResourceIfPossible(filenamePrefix)

  case class FilenamePolicy(baseFilename: String, shardTemplate: String = null,
                       templateSuffix: String = null, windowedWrites: Boolean = false)

  implicit def toFilenamePolicy(policy: FilenamePolicy): DefaultFilenamePolicy = {
    DefaultFilenamePolicy.fromStandardParameters(
      StaticValueProvider.of(policy.baseFilename),
      policy.shardTemplate,
      policy.templateSuffix,
      policy.windowedWrites)
  }

}

