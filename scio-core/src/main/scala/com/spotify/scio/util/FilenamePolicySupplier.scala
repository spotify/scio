/*
 * Copyright 2022 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.util

import com.twitter.chill.ClosureCleaner
import org.apache.beam.sdk.io.FileBasedSink
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, PaneInfo}

trait FilenamePolicySupplier {
  def apply(path: String, suffix: String): FilenamePolicy
}

object FilenamePolicySupplier {
  def apply(
    prefix: String,
    shardNameTemplate: String,
    isWindowed: Boolean
  ): FilenamePolicySupplier = (path: String, suffix: String) =>
    ScioUtil.defaultFilenamePolicy(
      path,
      Option(prefix).getOrElse("part"),
      shardNameTemplate,
      suffix,
      isWindowed
    )

  def resolve(
    filenamePolicySupplier: FilenamePolicySupplier,
    prefix: String,
    shardNameTemplate: String,
    isWindowed: Boolean
  ): FilenamePolicySupplier = {
    require(
      shardNameTemplate == null || filenamePolicySupplier == null,
      "shardNameTemplate and filenamePolicySupplier may not be used together"
    )
    require(
      prefix == null || filenamePolicySupplier == null,
      "prefix and filenamePolicySupplier may not be used together"
    )
    Option(filenamePolicySupplier)
      .getOrElse(FilenamePolicySupplier(prefix, shardNameTemplate, isWindowed))
  }

  def filenamePolicySupplierOf(
    windowed: (Int, Int, BoundedWindow, PaneInfo) => String = null,
    unwindowed: (Int, Int) => String = null
  ): FilenamePolicySupplier = { (path: String, suffix: String) =>
    val cleanWindowed = ClosureCleaner.clean(windowed)
    val cleanUnwindowed = ClosureCleaner.clean(unwindowed)
    new FilenamePolicy {
      val resource =
        FileBasedSink.convertToFileResourceIfPossible(ScioUtil.strippedPath(path))
      private def resolve(filename: String, outputFileHints: FileBasedSink.OutputFileHints) = {
        resource.getCurrentDirectory.resolve(
          filename + suffix + outputFileHints.getSuggestedFilenameSuffix,
          StandardResolveOptions.RESOLVE_FILE
        )
      }
      override def windowedFilename(
        shardNumber: Int,
        numShards: Int,
        window: BoundedWindow,
        paneInfo: PaneInfo,
        outputFileHints: FileBasedSink.OutputFileHints
      ): ResourceId = {
        if (cleanWindowed == null) throw new NotImplementedError()
        resolve(cleanWindowed(shardNumber, numShards, window, paneInfo), outputFileHints)
      }
      override def unwindowedFilename(
        shardNumber: Int,
        numShards: Int,
        outputFileHints: FileBasedSink.OutputFileHints
      ): ResourceId = {
        if (cleanUnwindowed == null) throw new NotImplementedError()
        resolve(cleanUnwindowed(shardNumber, numShards), outputFileHints)
      }
    }
  }
}
