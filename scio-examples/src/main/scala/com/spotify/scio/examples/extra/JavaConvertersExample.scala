/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
