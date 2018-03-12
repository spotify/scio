package com.spotify.scio

import com.spotify.scio.avro.types.AvroType
import org.apache.beam.sdk.extensions.gcp.storage.GcsResourceId
import org.apache.beam.sdk.io._
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.util.gcsfs.GcsPath

object JavaConverters {

  implicit def toResourceId(filenamePrefix: String): ResourceId =
    FileBasedSink.convertToFileResourceIfPossible(filenamePrefix)

}

