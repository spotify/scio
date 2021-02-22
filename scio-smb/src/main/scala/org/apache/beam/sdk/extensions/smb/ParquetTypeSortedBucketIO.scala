/*
 * Copyright 2021 Spotify AB.
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
package org.apache.beam.sdk.extensions.smb

import com.spotify.scio.coders.Coder
import magnolify.parquet.ParquetType
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.{BucketedInput, Predicate}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.values.TupleTag
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

object ParquetTypeSortedBucketIO {
  private val DefaultSuffix = ".parquet"

  def read[T: Coder: ParquetType](tupleTag: TupleTag[T]): Read[T] = Read(tupleTag)

  def write[K: ClassTag, T: ClassTag: Coder: ParquetType](keyField: String): Write[K, T] =
    Write(keyField)

  def transformOutput[K: ClassTag, T: ClassTag: Coder: ParquetType](
    keyField: String
  ): TransformOutput[K, T] =
    TransformOutput(keyField)

  case class Read[T: Coder: ParquetType](
    tupleTag: TupleTag[T],
    inputDirectories: Seq[ResourceId] = Nil,
    filenameSuffix: String = DefaultSuffix,
    filterPredicate: FilterPredicate = null,
    predicate: Predicate[T] = null
  ) extends SortedBucketIO.Read[T] {
    def from(inputDirectories: String*): Read[T] =
      this.copy(inputDirectories = inputDirectories.map(FileSystems.matchNewResource(_, true)))

    def withSuffix(filenameSuffix: String): Read[T] =
      this.copy(filenameSuffix = filenameSuffix)

    def withFilterPredicate(filterPredicate: FilterPredicate): Read[T] =
      this.copy(filterPredicate = filterPredicate)

    def withPredicate(predicate: Predicate[T]): Read[T] =
      this.copy(predicate = predicate)

    override def getTupleTag: TupleTag[T] = tupleTag

    override protected def toBucketedInput: SortedBucketSource.BucketedInput[_, T] = {
      val fileOperations = ParquetTypeFileOperations[T](filterPredicate)
      new BucketedInput(
        getTupleTag,
        inputDirectories.asJava,
        filenameSuffix,
        fileOperations,
        predicate
      )
    }
  }

  case class Write[K: ClassTag, T: ClassTag: Coder: ParquetType](
    keyField: String,
    compression: CompressionCodecName = ParquetTypeFileOperations.DefaultCompression,
    configuration: Configuration = new Configuration(),
    numBuckets: Int = SortedBucketIO.DEFAULT_NUM_BUCKETS,
    numShards: Int = SortedBucketIO.DEFAULT_NUM_SHARDS,
    filenamePrefix: String = SortedBucketIO.DEFAULT_FILENAME_PREFIX,
    hashType: HashType = SortedBucketIO.DEFAULT_HASH_TYPE,
    outputDirectory: ResourceId = null,
    tempDirectory: ResourceId = null,
    filenameSuffix: String = DefaultSuffix,
    sorterMemoryMb: Int = SortedBucketIO.DEFAULT_SORTER_MEMORY_MB,
    keyCacheSize: Int = 0
  ) extends SortedBucketIO.Write[K, T] {
    private val keyClass = implicitly[ClassTag[K]].runtimeClass.asInstanceOf[Class[K]]
    private val recordClass = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]

    def withNumBuckets(numBuckets: Int): Write[K, T] =
      this.copy(numBuckets = numBuckets)

    def withNumShards(numShards: Int): Write[K, T] =
      this.copy(numShards = numShards)

    def withHashType(hashType: HashType): Write[K, T] =
      this.copy(hashType = hashType)

    def to(outputDirectory: String): Write[K, T] =
      this.copy(outputDirectory = FileSystems.matchNewResource(outputDirectory, true))

    def withTempDirectory(tempDirectory: String): Write[K, T] =
      this.copy(tempDirectory = FileSystems.matchNewResource(tempDirectory, true))

    def withFilenamePrefix(filenamePrefix: String): Write[K, T] =
      this.copy(filenamePrefix = filenamePrefix)

    def withSuffix(filenameSuffix: String): Write[K, T] =
      this.copy(filenameSuffix = filenameSuffix)

    def withSorterMemoryMb(sorterMemoryMb: Int): Write[K, T] =
      this.copy(sorterMemoryMb = sorterMemoryMb)

    def withKeyCacheOfSize(keyCacheSize: Int): Write[K, T] =
      this.copy(keyCacheSize = keyCacheSize)

    def withCompression(compression: CompressionCodecName): Write[K, T] =
      this.copy(compression = compression)

    def withConfiguration(configuration: Configuration): Write[K, T] =
      this.copy(configuration = configuration)

    override def getNumBuckets: Int = numBuckets
    override def getNumShards: Int = numShards
    override def getFilenamePrefix: String = filenamePrefix
    override def getKeyClass: Class[K] = keyClass
    override def getHashType: HashType = hashType
    override def getOutputDirectory: ResourceId = outputDirectory
    override def getTempDirectory: ResourceId = tempDirectory
    override def getFilenameSuffix: String = filenameSuffix
    override def getSorterMemoryMb: Int = sorterMemoryMb
    override def getFileOperations: FileOperations[T] =
      ParquetTypeFileOperations[T](compression, configuration)

    override def getBucketMetadata: BucketMetadata[K, T] =
      new ParquetBucketMetadata[K, T](
        numBuckets,
        numShards,
        keyClass,
        hashType,
        keyField,
        filenamePrefix,
        recordClass
      )

    override def getKeyCacheSize: Int = keyCacheSize
  }

  case class TransformOutput[K: ClassTag, T: ClassTag: Coder: ParquetType](
    keyField: String,
    compression: CompressionCodecName = ParquetTypeFileOperations.DefaultCompression,
    configuration: Configuration = new Configuration(),
    filenamePrefix: String = SortedBucketIO.DEFAULT_FILENAME_PREFIX,
    outputDirectory: ResourceId = null,
    tempDirectory: ResourceId = null,
    filenameSuffix: String = DefaultSuffix
  ) extends SortedBucketIO.TransformOutput[K, T] {
    private val keyClass = implicitly[ClassTag[K]].runtimeClass.asInstanceOf[Class[K]]
    private val recordClass = implicitly[ClassTag[T]].runtimeClass.asInstanceOf[Class[T]]

    def to(outputDirectory: String): TransformOutput[K, T] =
      this.copy(outputDirectory = FileSystems.matchNewResource(outputDirectory, true))

    def withTempDirectory(tempDirectory: String): TransformOutput[K, T] =
      this.copy(tempDirectory = FileSystems.matchNewResource(tempDirectory, true))

    def withSuffix(filenameSuffix: String): TransformOutput[K, T] =
      this.copy(filenameSuffix = filenameSuffix)

    def withFilenamePrefix(filenamePrefix: String): TransformOutput[K, T] =
      this.copy(filenamePrefix = filenamePrefix)

    def withCompression(compression: CompressionCodecName): TransformOutput[K, T] =
      this.copy(compression = compression)

    def withConfiguration(configuration: Configuration): TransformOutput[K, T] =
      this.copy(configuration = configuration)

    override def getKeyClass: Class[K] = keyClass
    override def getOutputDirectory: ResourceId = outputDirectory
    override def getTempDirectory: ResourceId = tempDirectory
    override def getFilenameSuffix: String = filenameSuffix
    override def getFilenamePrefix: String = filenamePrefix
    override def getFileOperations: FileOperations[T] =
      ParquetTypeFileOperations(compression, configuration)

    override def getNewBucketMetadataFn: SortedBucketTransform.NewBucketMetadataFn[K, T] = {
      val _keyField = keyField
      val _keyClass = keyClass
      val _recordClass = recordClass
      val _filenamePrefix = filenamePrefix

      (numBuckets, numShards, hashType) => {
        new ParquetBucketMetadata[K, T](
          numBuckets,
          numShards,
          _keyClass,
          hashType,
          _keyField,
          _filenamePrefix,
          _recordClass
        )
      }
    }
  }
}
