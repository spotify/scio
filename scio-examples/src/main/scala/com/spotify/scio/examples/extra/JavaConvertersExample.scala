package com.spotify.scio.examples.extra

import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import com.spotify.scio.JavaConverters._

object JavaConvertersExample {

  val path: String = "gs://foobar/path/to/file"
  TextIO.writeCustomType().toResource(StaticValueProvider.of(path))

}
