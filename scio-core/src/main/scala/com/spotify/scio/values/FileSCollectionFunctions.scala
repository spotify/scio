/*
 * Copyright 2024 Spotify AB
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

package com.spotify.scio.values

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.ReadIO
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.util.Functions
import com.spotify.scio.util.TupleFunctions.kvToTuple
import org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment
import org.apache.beam.sdk.{io => beam}
import org.apache.beam.sdk.io.{
  Compression,
  FileBasedSource,
  ReadAllViaFileBasedSource,
  ReadAllViaFileBasedSourceWithFilename
}
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.transforms.{PTransform, ParDo}
import org.apache.beam.sdk.values.{KV, PCollection}

object FileSCollectionFunctions {

  // from beam TextIO/AvroIO
  // 64MB is a reasonable value that allows to amortize the cost of opening files,
  // but is not so large as to exhaust a typical runner's maximum amount of output per ProcessElement call.
  val DefaultBundleSizeBytes: Long = 64 * 1024 * 1024L
}

class FileSCollectionFunctions(self: SCollection[String]) {

  import FileSCollectionFunctions._

  /**
   * Reads each file, represented as a pattern, in this [[SCollection]].
   *
   * @return
   *   each line of the input files.
   */
  def readTextFiles(): SCollection[String] =
    readFiles(beam.TextIO.readFiles())

  /**
   * Reads each file, represented as a pattern, in this [[SCollection]].
   *
   * @return
   *   each file fully read as [[Array[Byte]].
   */
  def readFilesAsBytes(): SCollection[Array[Byte]] =
    readFiles(_.readFullyAsBytes())

  /**
   * Reads each file, represented as a pattern, in this [[SCollection]].
   *
   * @return
   *   each file fully read as [[String]].
   */
  def readFilesAsString(): SCollection[String] =
    readFiles(_.readFullyAsUTF8String())

  /**
   * Reads each file, represented as a pattern, in this [[SCollection]].
   *
   * @see
   *   [[readFilesAsBytes]], [[readFilesAsString]]
   */
  def readFiles[A: Coder](
    f: beam.FileIO.ReadableFile => A
  ): SCollection[A] =
    readFiles(DirectoryTreatment.SKIP, Compression.AUTO)(f)

  /**
   * Reads each file, represented as a pattern, in this [[SCollection]].
   *
   * @see
   *   [[readFilesAsBytes]], [[readFilesAsString]]
   *
   * @param directoryTreatment
   *   Controls how to handle directories in the input.
   * @param compression
   *   Reads files using the given [[org.apache.beam.sdk.io.Compression]].
   */
  def readFiles[A: Coder](directoryTreatment: DirectoryTreatment, compression: Compression)(
    f: beam.FileIO.ReadableFile => A
  ): SCollection[A] = {
    val transform =
      ParDo
        .of(Functions.mapFn[beam.FileIO.ReadableFile, A](f))
        .asInstanceOf[PTransform[PCollection[beam.FileIO.ReadableFile], PCollection[A]]]
    readFiles(transform, directoryTreatment, compression)
  }

  /**
   * Reads each file, represented as a pattern, in this [[SCollection]]. Files are split into
   * multiple offset ranges and read with the [[FileBasedSource]].
   *
   * @param desiredBundleSizeBytes
   *   Desired size of bundles read by the sources.
   * @param directoryTreatment
   *   Controls how to handle directories in the input.
   * @param compression
   *   Reads files using the given [[org.apache.beam.sdk.io.Compression]].
   */
  def readFiles[A: Coder](
    desiredBundleSizeBytes: Long,
    directoryTreatment: DirectoryTreatment,
    compression: Compression
  )(f: String => FileBasedSource[A]): SCollection[A] = {
    val createSource = Functions.serializableFn(f)
    val bcoder = CoderMaterializer.beam(self.context, Coder[A])
    val fileTransform = new ReadAllViaFileBasedSource(desiredBundleSizeBytes, createSource, bcoder)
    readFiles(fileTransform, directoryTreatment, compression)
  }

  /**
   * Reads each file, represented as a pattern, in this [[SCollection]].
   *
   * @see
   *   [[readFilesAsBytes]], [[readFilesAsString]], [[readFiles]]
   *
   * @param directoryTreatment
   *   Controls how to handle directories in the input.
   * @param compression
   *   Reads files using the given [[org.apache.beam.sdk.io.Compression]].
   */
  def readFiles[A: Coder](
    filesTransform: PTransform[PCollection[beam.FileIO.ReadableFile], PCollection[A]],
    directoryTreatment: DirectoryTreatment = DirectoryTreatment.SKIP,
    compression: Compression = Compression.AUTO
  ): SCollection[A] =
    if (self.context.isTest) {
      val id = self.context.testId.get
      self.flatMap(s => TestDataManager.getInput(id)(ReadIO[A](s)).asIterable.get)
    } else {
      self
        .applyTransform(new PTransform[PCollection[String], PCollection[A]]() {
          override def expand(input: PCollection[String]): PCollection[A] =
            input
              .apply(beam.FileIO.matchAll())
              .apply(
                beam.FileIO
                  .readMatches()
                  .withCompression(compression)
                  .withDirectoryTreatment(directoryTreatment)
              )
              .apply(filesTransform)
        })
    }

  /**
   * Reads each file, represented as a pattern, in this [[SCollection]]. Files are split into
   * multiple offset ranges and read with the [[FileBasedSource]].
   *
   * @return
   *   origin file name paired with read line.
   *
   * @param desiredBundleSizeBytes
   *   Desired size of bundles read by the sources.
   * @param directoryTreatment
   *   Controls how to handle directories in the input.
   * @param compression
   *   Reads files using the given [[org.apache.beam.sdk.io.Compression]].
   */
  def readTextFilesWithPath(
    desiredBundleSize: Long = DefaultBundleSizeBytes,
    directoryTreatment: DirectoryTreatment = DirectoryTreatment.SKIP,
    compression: Compression = Compression.AUTO
  ): SCollection[(String, String)] = {
    readFilesWithPath(
      desiredBundleSize,
      directoryTreatment,
      compression
    ) { f =>
      new beam.TextSource(
        StaticValueProvider.of(f),
        EmptyMatchTreatment.DISALLOW,
        Array('\n'.toByte),
        0
      )
    }
  }

  /**
   * Reads each file, represented as a pattern, in this [[SCollection]]. Files are split into
   * multiple offset ranges and read with the [[FileBasedSource]].
   *
   * @return
   *   origin file name paired with read element.
   *
   * @param desiredBundleSizeBytes
   *   Desired size of bundles read by the sources.
   * @param directoryTreatment
   *   Controls how to handle directories in the input.
   * @param compression
   *   Reads files using the given [[org.apache.beam.sdk.io.Compression]].
   */
  def readFilesWithPath[A: Coder](
    desiredBundleSizeBytes: Long = DefaultBundleSizeBytes,
    directoryTreatment: DirectoryTreatment = DirectoryTreatment.SKIP,
    compression: Compression = Compression.AUTO
  )(
    f: String => FileBasedSource[A]
  ): SCollection[(String, A)] = {
    if (self.context.isTest) {
      val id = self.context.testId.get
      self.flatMap { s =>
        TestDataManager
          .getInput(id)(ReadIO[A](s))
          .asIterable
          .get
          .map(x => s -> x)
      }
    } else {
      val createSource = Functions.serializableFn(f)
      val bcoder = CoderMaterializer.beam(self.context, Coder[KV[String, A]])
      val fileTransform = new ReadAllViaFileBasedSourceWithFilename(
        desiredBundleSizeBytes,
        createSource,
        bcoder
      )
      readFiles(fileTransform, directoryTreatment, compression).map(kvToTuple)
    }
  }

}
