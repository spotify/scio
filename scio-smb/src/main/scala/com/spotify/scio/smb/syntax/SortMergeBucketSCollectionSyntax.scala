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

package com.spotify.scio.smb.syntax

import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values._
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType
import org.apache.beam.sdk.extensions.smb.{AvroSortedBucketIO, SortedBucketSink}

import scala.reflect.ClassTag

trait SortMergeBucketSCollectionSyntax {
  implicit def toGenericAvroCollection(
    data: SCollection[_ <: GenericRecord]
  ): SortedBucketGenericAvroSCollection =
    new SortedBucketGenericAvroSCollection(data.asInstanceOf[SCollection[GenericRecord]])

  implicit def toSpecificAvroCollection[T <: SpecificRecordBase: ClassTag: Coder](
    data: SCollection[T]
  ): SortedBucketSpecificAvroSCollection[T] =
    new SortedBucketSpecificAvroSCollection[T](data)
}

private[smb] object SortMergeBucketSCollectionSyntax {
  // Adapted from ScioUtil
  private[smb] def toPathFragment(path: String): String =
    if (path.endsWith("/")) s"${path}bucket-*-shard-*.avro"
    else s"$path/bucket-*.avro"
}

final class SortedBucketGenericAvroSCollection(private val self: SCollection[GenericRecord]) {
  import com.spotify.scio.avro.{GenericRecordIO, GenericRecordTap}
  import com.spotify.scio.smb.syntax.SortMergeBucketSCollectionSyntax._
  import org.apache.avro.Schema
  import org.apache.avro.file.CodecFactory

  def saveAsSortedBucket[K: Coder: ClassTag](
    outputDirectory: String,
    schema: Schema,
    keyField: String,
    hashType: HashType,
    numBuckets: Int,
    numShards: Int = 1,
    codec: CodecFactory = CodecFactory.snappyCodec()
  ): GenericRecordTap = {
    if (self.context.isTest) {
      TestDataManager.getOutput(self.context.testId.get)(
        GenericRecordIO(outputDirectory, schema)
      )(self)
    } else {
      self.applyInternal(
        AvroSortedBucketIO
          .write(ScioUtil.classOf[K], keyField, schema)
          .to(outputDirectory)
          .withTempDirectory(self.context.options.getTempLocation)
          .withCodec(codec)
          .withHashType(hashType)
          .withNumBuckets(numBuckets)
          .withNumShards(numShards)
      )
    }

    GenericRecordTap(toPathFragment(outputDirectory), schema)
  }
}

final class SortedBucketSpecificAvroSCollection[T <: SpecificRecordBase: ClassTag: Coder](
  private val self: SCollection[T]
) {
  import com.spotify.scio.avro.{SpecificRecordIO, SpecificRecordTap}
  import com.spotify.scio.smb.syntax.SortMergeBucketSCollectionSyntax._
  import org.apache.avro.file.CodecFactory

  def saveAsSortedBucket[K: Coder: ClassTag](
    outputDirectory: String,
    keyField: String,
    hashType: String,
    numBuckets: Int,
    numShards: Int = 1,
    codec: CodecFactory = CodecFactory.snappyCodec()
  ): SpecificRecordTap[T] = {
    if (self.context.isTest) {
      TestDataManager.getOutput(self.context.testId.get)(
        SpecificRecordIO[T](outputDirectory)
      )(self)
    } else {
      self.applyInternal[SortedBucketSink.WriteResult](
        AvroSortedBucketIO
          .write[K, T](ScioUtil.classOf[K], keyField, ScioUtil.classOf[T])
          .to(outputDirectory)
          .withTempDirectory(self.context.options.getTempLocation)
          .withCodec(codec)
          .withNumBuckets(numBuckets)
          .withNumShards(numShards)
      )
    }

    SpecificRecordTap[T](toPathFragment(outputDirectory))
  }
}
