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
  def filenamePolicySupplierOf(
    windowed: (Int, Int, BoundedWindow, PaneInfo) => String = null,
    unwindowed: (Int, Int) => String = null
  ): FilenamePolicySupplier = { (path: String, suffix: String) =>
    val cleanWindowed = ClosureCleaner.clean(windowed)
    val cleanUnwindowed = ClosureCleaner.clean(unwindowed)
    new FilenamePolicy {
      val resource =
        FileBasedSink.convertToFileResourceIfPossible(ScioUtil.pathWithPrefix(path, ""))
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
