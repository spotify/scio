/*
 * Copyright 2023 Spotify AB
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

package com.spotify.scio.smb

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{KeyedIO, Tap, TapOf, TapT, TestIO}
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.extensions.smb.{AvroSortedBucketIO, BucketShardId, FileOperations}
import org.apache.beam.sdk.extensions.smb.SortedBucketSink.WriteResult
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.values.{KV, PCollection, TupleTag}

import java.nio.file.Files
import scala.jdk.CollectionConverters._

final class SmbIO[K, T](path: String, override val keyBy: T => K)(implicit
  override val keyCoder: Coder[K]
) extends TestIO[T]
    with KeyedIO[T] {
  override type KeyT = K
  override val tapT: TapT.Aux[T, T] = TapOf[T]
  override def testId: String = SmbIO.testId(path)
}

object SmbIO {
  def apply[K: Coder, T](path: String, keyBy: T => K): SmbIO[K, T] =
    new SmbIO[K, T](path, keyBy)

  /**
   * Prepare SMB test inputs by writing test data as real SMB files.
   *
   * Call this before running a test that uses SMBCollection directly:
   * {{{
   * SmbIO.prepareTestInput("gs://users", classOf[Integer], "id", userData)
   * SmbIO.prepareTestInput("gs://accounts", classOf[Integer], "id", accountData)
   * JobTest[MyJob.type].input(...).run()
   * }}}
   *
   * @param path
   *   Logical path used in test
   * @param keyClass
   *   Key class for bucketing
   * @param keyField
   *   Field name for the key
   * @param data
   *   Test data to write
   */
  def prepareTestInput[K, T <: org.apache.avro.specific.SpecificRecord: Coder](
    path: String,
    keyClass: Class[K],
    keyField: String,
    data: Iterable[T]
  ): Unit = {
    writeTestInput(path, keyClass, keyField, data)
    ()
  }

  def testId(paths: String*): String = {
    val normalizedPaths = paths.map(ScioUtil.strippedPath).sorted.mkString(",")
    s"SmbIO($normalizedPaths)"
  }

  /**
   * Resolve a path to its physical location, handling test mode path mapping.
   *
   * In test mode, logical paths (like "gs://users") are resolved to temp directories containing
   * actual SMB files. In production mode, paths are returned unchanged.
   *
   * This enables JobTest to work with SMBCollection by writing test data as real SMB files.
   *
   * Note: This method can only resolve paths if test data has been registered via
   * `.input(SmbIO(...), data)` AND prepareTestInput has been called manually. Automatic resolution
   * from TestDataManager is not currently supported due to the need for key class and field name
   * information.
   *
   * @param path
   *   Logical path (production) or test path
   * @param sc
   *   Scio context (to check if in test mode)
   * @return
   *   Physical path to read from
   */
  def resolvePath(path: String)(implicit sc: ScioContext): String = {
    if (sc.isTest) {
      SmbTestPaths.resolve(testId(path)).getOrElse(path)
    } else {
      path
    }
  }

  /**
   * Write test data as SMB files to a temp directory and register the path.
   *
   * This is called during JobTest setup to prepare test inputs for SMBCollection.
   *
   * @param path
   *   Logical path (e.g., "gs://users")
   * @param keyClass
   *   Key class for bucketing
   * @param keyField
   *   Field name for the key
   * @param data
   *   Test data to write
   * @tparam K
   *   Key type
   * @tparam T
   *   Record type
   * @return
   *   Physical path to the temp directory
   */
  private[scio] def writeTestInput[K, T <: org.apache.avro.specific.SpecificRecord: Coder](
    path: String,
    keyClass: Class[K],
    keyField: String,
    data: Iterable[T]
  ): String = {
    if (data.isEmpty) {
      // For empty data, just create an empty directory
      val tempDir = Files.createTempDirectory("smb-test-empty").toFile
      val tempPath = tempDir.getAbsolutePath
      SmbTestPaths.register(testId(path), tempPath)
      return tempPath
    }

    // Create temp directory
    val tempDir = Files.createTempDirectory("smb-test").toFile
    val tempPath = tempDir.getAbsolutePath

    // Write data as SMB files
    val sc = ScioContext()
    val recordClass = data.head.getClass.asInstanceOf[Class[T]]

    sc.parallelize(data.toSeq)
      .saveAsSortedBucket(
        AvroSortedBucketIO
          .write(keyClass, keyField, recordClass)
          .to(tempPath)
          .withNumBuckets(2)
          .withNumShards(1)
      )
    sc.run().waitUntilDone()

    // Register path mapping
    SmbTestPaths.register(testId(path), tempPath)

    tempPath
  }

  // Overload for SMBCollection (uses OutputInfo to avoid materializing bucket files)
  private[scio] def tap[T: Coder](
    outputInfo: com.spotify.scio.smb.SMBCollectionImpl.OutputInfo
  ): ScioContext => Tap[T] =
    (_: ScioContext) => {
      // Create a lazy tap that doesn't scan files until accessed
      new Tap[T] {
        override def value: Iterator[T] = {
          // Use FileOperations directly - no reconstruction needed!
          val fileOps = outputInfo.fileOperations

          // Scan output directory for bucket files (files now exist because pipeline has run)
          val filePattern =
            s"${outputInfo.outputDirectory}/${outputInfo.filenamePrefix}*${outputInfo.filenameSuffix}"

          import org.apache.beam.sdk.io.FileSystems
          import scala.jdk.CollectionConverters._

          val matchResult = FileSystems.`match`(filePattern)
          val bucketFiles = matchResult.metadata().asScala.map(_.resourceId())

          bucketFiles.iterator.flatMap(resourceId =>
            fileOps.iterator(resourceId).asScala.asInstanceOf[Iterator[T]]
          )
        }
        override def open(sc: ScioContext): com.spotify.scio.values.SCollection[T] =
          throw new UnsupportedOperationException("SMB taps cannot be reopened")
      }
    }

  // Overload for Write (used by regular .saveAsSortedBucket)
  private[scio] def tap[K1, K2, T: Coder](
    write: org.apache.beam.sdk.extensions.smb.SortedBucketIO.Write[K1, K2, T],
    writeResult: WriteResult
  ): ScioContext => Tap[T] =
    tap(write.getFileOperations(), writeResult)

  // Overload for FileOperations (used by old SortMergeBucketSCollectionSyntax)
  private[scio] def tap[T: Coder](
    fileOperations: FileOperations[T],
    writeResult: WriteResult
  ): ScioContext => Tap[T] =
    (sc: ScioContext) => {
      // Create coder from FileOperations to preserve DatumFactory
      val valueCoder: Coder[T] = Coder.beam[T](fileOperations.getCoder())

      val bucketFiles = sc
        .wrap(
          writeResult
            .expand()
            .get(new TupleTag("WrittenFiles"))
            .asInstanceOf[PCollection[KV[BucketShardId, ResourceId]]]
        )
        .materialize

      bucketFiles.underlying.flatMap(kv => fileOperations.iterator(kv.getValue).asScala)(valueCoder)
    }
}
