package com.spotify.scio.examples.extra

import org.apache.beam.sdk.io.{AvroIO, TextIO}
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import com.spotify.scio.JavaConverters._

object JavaConvertersExample {

  val path: String = "gs://foobar/path/to/file"
  TextIO.writeCustomType().toResource(StaticValueProvider.of(path))

  AvroIO.writeCustomType().to(FilenamePolicy(path))
  TextIO.writeCustomType().to(FilenamePolicy(path, "-SSSSS-of-NNNNN", ".csv", true))
  AvroIO.writeCustomType().to(FilenamePolicy(path, templateSuffix = ".tsv"))

}
