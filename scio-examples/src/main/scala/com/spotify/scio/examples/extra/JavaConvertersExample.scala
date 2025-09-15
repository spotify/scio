/*
 * Copyright 2019 Spotify AB.
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

// Example: Java converters for various output formats
// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.JavaConvertersExample
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
// --output=gs://[OUTPUT] --converter=[CONVERTER]"`
package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.JavaConverters._
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider

object JavaConvertersExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val output = args("output")
    val t: TextIO.TypedWrite[String, Void] = TextIO.writeCustomType()
    val transform = args("converter") match {
      case "String#toResourceId" =>
        t.toResource(StaticValueProvider.of(output.toResourceId))
      case "String#toFilenamePolicy"      => t.to(output.toFilenamePolicy)
      case "String#toStaticValueProvider" => t.to(output.toStaticValueProvider)
      case "FilenamePolicy#toJava"        =>
        t.to(FilenamePolicy(output, "-SSSSS-of-NNNNN", ".csv").asJava)
    }

    // In prod, `TextIO` transform converts `Int` -> `String` before write.
    // Test does not apply transform, compares SCol prior to output to expected. Need manual cast.
    sc.parallelize(1 to 10)
      .map(_.toString)
      .saveAsCustomOutput(output, transform)
    sc.run()
    ()
  }
}
