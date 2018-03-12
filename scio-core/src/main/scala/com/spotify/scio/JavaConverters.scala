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

package com.spotify.scio

import org.apache.beam.sdk.io.{DefaultFilenamePolicy, FileBasedSink}
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider

object JavaConverters {

  implicit def toResourceId(filenamePrefix: String): ResourceId =
    FileBasedSink.convertToFileResourceIfPossible(filenamePrefix)

  case class FilenamePolicy(baseFilename: String,
                            shardTemplate: String = null,
                            templateSuffix: String = null,
                            windowedWrites: Boolean = false)

  implicit def toFilenamePolicy(policy: FilenamePolicy): DefaultFilenamePolicy = {
    DefaultFilenamePolicy.fromStandardParameters(
      StaticValueProvider.of(policy.baseFilename),
      policy.shardTemplate,
      policy.templateSuffix,
      policy.windowedWrites)
  }

}

