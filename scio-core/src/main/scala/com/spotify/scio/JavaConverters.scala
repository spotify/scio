/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio

import org.apache.beam.sdk.io.{DefaultFilenamePolicy, FileBasedSink}
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider

/**
 * Converters for Beam Java SDK APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.JavaConverters._
 * }}}
 */
object JavaConverters {

  /** Enhanced version of [[String]] with Beam Java SDK converter methods. */
  implicit class RichString(private val s: String) extends AnyVal {

    /** Convert the string to a [[ResourceId]]. */
    def toResourceId: ResourceId =
      FileBasedSink.convertToFileResourceIfPossible(s)

    def toFilenamePolicy: DefaultFilenamePolicy =
      DefaultFilenamePolicy.fromStandardParameters(StaticValueProvider.of(s.toResourceId),
                                                   null,
                                                   null,
                                                   false)
  }

  /** Scio version of [[DefaultFilenamePolicy]]. */
  final case class FilenamePolicy(baseFilename: String,
                                  shardTemplate: String = null,
                                  templateSuffix: String = null,
                                  windowedWrites: Boolean = false) {

    /**
     * Convert the filename policy to a
     * [[org.apache.beam.sdk.io.DefaultFilenamePolicy DefaultFilenamePolicy]].
     */
    def asJava: DefaultFilenamePolicy =
      DefaultFilenamePolicy.fromStandardParameters(
        StaticValueProvider.of(baseFilename.toResourceId),
        shardTemplate,
        templateSuffix,
        windowedWrites)
  }

  /** Enhanced version of [[Any]] with Beam Java SDK converter methods. */
  implicit class RichAny[T](private val value: T) extends AnyVal {

    /**
     * Convert the value to a
     * [[org.apache.beam.sdk.options.ValueProvider.StaticValueProvider StaticValueProvider]].
     */
    def toStaticValueProvider: StaticValueProvider[T] =
      StaticValueProvider.of(value)
  }

}
