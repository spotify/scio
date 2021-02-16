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

import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType
import org.apache.beam.sdk.io.AvroGeneratedUser
import org.apache.beam.sdk.transforms.display.DisplayData
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import java.nio.charset.Charset
import scala.jdk.CollectionConverters._

object ParquetBucketMetadataTest {
  case class Location(postalCode: Int, countryId: String)
  case class User(id: Long, name: String, location: Location)
}

class ParquetBucketMetadataTest extends AnyFlatSpec with Matchers {
  import AvroBucketMetadataTest.LOCATION_SCHEMA
  import AvroBucketMetadataTest.RECORD_SCHEMA

  "ParquetBucketMetadata" should "work with generic records" in {
    val countryId = ByteBuffer.wrap("US".getBytes(Charset.defaultCharset()))
    val postalCode = ByteBuffer.wrap("11".getBytes(Charset.defaultCharset()))
    val location = new GenericRecordBuilder(LOCATION_SCHEMA)
      .set("countryId", countryId)
      .set("prevCountries", List("CN", "MX").asJava)
      .set("postalCode", postalCode)
      .build()

    val user = new GenericRecordBuilder(RECORD_SCHEMA)
      .set("id", 10L)
      .set("location", location)
      .set("locationUnion", location)
      .set("suffix", "Jr")
      .build()

    val idMeta = new ParquetBucketMetadata[Long, GenericRecord](
      1,
      1,
      classOf[Long],
      HashType.MURMUR3_32,
      "id",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      RECORD_SCHEMA
    )
    idMeta.extractKey(user) shouldBe 10L

    val countryIdMeta = new ParquetBucketMetadata[ByteBuffer, GenericRecord](
      1,
      1,
      classOf[ByteBuffer],
      HashType.MURMUR3_32,
      "location.countryId",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      RECORD_SCHEMA
    )
    countryIdMeta.extractKey(user) shouldBe countryId

    val postalCodeMeta = new ParquetBucketMetadata[ByteBuffer, GenericRecord](
      1,
      1,
      classOf[ByteBuffer],
      HashType.MURMUR3_32,
      "location.postalCode",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      RECORD_SCHEMA
    )
    postalCodeMeta.extractKey(user) shouldBe postalCode

    val suffixMeta = new ParquetBucketMetadata[String, GenericRecord](
      1,
      1,
      classOf[String],
      HashType.MURMUR3_32,
      "suffix",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      RECORD_SCHEMA
    )
    suffixMeta.extractKey(user) shouldBe "Jr"
  }

  it should "work with specific records" in {
    val user = new AvroGeneratedUser("foo", 50, "green")

    val colorMeta = new ParquetBucketMetadata[String, AvroGeneratedUser](
      1,
      1,
      classOf[String],
      HashType.MURMUR3_32,
      "favorite_color",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )
    colorMeta.extractKey(user) shouldBe "green"

    val numberMeta = new ParquetBucketMetadata[Int, AvroGeneratedUser](
      1,
      1,
      classOf[Int],
      HashType.MURMUR3_32,
      "favorite_number",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )
    numberMeta.extractKey(user) shouldBe 50
  }

  it should "work with case classes" in {
    import ParquetBucketMetadataTest._
    val user = User(10L, "Johnny", Location(12345, "US"))

    val idMeta = new ParquetBucketMetadata[Long, User](
      1,
      1,
      classOf[Long],
      HashType.MURMUR3_32,
      "id",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[User]
    )
    idMeta.extractKey(user) shouldBe 10L

    val countryIdMeta = new ParquetBucketMetadata[String, User](
      1,
      1,
      classOf[String],
      HashType.MURMUR3_32,
      "location.countryId",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[User]
    )
    countryIdMeta.extractKey(user) shouldBe "US"

    val postalCodeMeta = new ParquetBucketMetadata[Int, User](
      1,
      1,
      classOf[Int],
      HashType.MURMUR3_32,
      "location.postalCode",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[User]
    )
    postalCodeMeta.extractKey(user) shouldBe 12345
  }

  it should "round trip" in {
    val metadata = new ParquetBucketMetadata[String, AvroGeneratedUser](
      1,
      1,
      1,
      classOf[String],
      HashType.MURMUR3_32,
      "favorite_color",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX
    )
    val copy = BucketMetadata.from(metadata.toString)
    copy.getVersion shouldBe metadata.getVersion
    copy.getNumBuckets shouldBe metadata.getNumBuckets
    copy.getNumShards shouldBe metadata.getNumShards
    copy.getKeyClass shouldBe metadata.getKeyClass
    copy.getHashType shouldBe metadata.getHashType
  }

  it should "have default version" in {
    val metadata = new ParquetBucketMetadata(
      1,
      1,
      classOf[String],
      HashType.MURMUR3_32,
      "favorite_color",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )
    metadata.getVersion shouldBe BucketMetadata.CURRENT_VERSION
  }

  it should "have display data" in {
    val metadata = new ParquetBucketMetadata(
      2,
      1,
      classOf[String],
      HashType.MURMUR3_32,
      "favorite_color",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )
    val displayData = DisplayData.from(metadata).asMap().asScala.map { kv =>
      kv._1.getKey -> kv._2.getValue
    }
    displayData.get("numBuckets") shouldBe Some(2)
    displayData.get("numShards") shouldBe Some(1)
    displayData.get("version") shouldBe Some(BucketMetadata.CURRENT_VERSION)
    displayData.get("keyField") shouldBe Some("favorite_color")
    displayData.get("keyClass") shouldBe Some(classOf[String].getName)
    displayData.get("hashType") shouldBe Some(HashType.MURMUR3_32.toString)
    displayData.get("keyCoder") shouldBe Some(classOf[StringUtf8Coder].getName)
  }

  it should "check source compatibility" in {
    val metadata1 = new ParquetBucketMetadata[String, AvroGeneratedUser](
      2,
      1,
      classOf[String],
      HashType.MURMUR3_32,
      "name",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )
    val metadata2 = new ParquetBucketMetadata[String, AvroGeneratedUser](
      2,
      1,
      classOf[String],
      HashType.MURMUR3_32,
      "favorite_color",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )
    val metadata3 = new ParquetBucketMetadata[String, AvroGeneratedUser](
      4,
      1,
      classOf[String],
      HashType.MURMUR3_32,
      "favorite_color",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )
    val metadata4 = new ParquetBucketMetadata[Int, AvroGeneratedUser](
      4,
      1,
      classOf[Int],
      HashType.MURMUR3_32,
      "favorite_number",
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )

    metadata1.isPartitionCompatible(metadata2) shouldBe false
    metadata2.isPartitionCompatible(metadata3) shouldBe true
    metadata3.isPartitionCompatible(metadata4) shouldBe false
  }

  it should "check key type" in {
    an[IllegalArgumentException] shouldBe thrownBy {
      new ParquetBucketMetadata[String, GenericRecord](
        1,
        1,
        classOf[String],
        HashType.MURMUR3_32,
        "location.countryId",
        SortedBucketIO.DEFAULT_FILENAME_PREFIX,
        RECORD_SCHEMA
      )
    }

    an[IllegalArgumentException] shouldBe thrownBy {
      import ParquetBucketMetadataTest._
      new ParquetBucketMetadata[ByteBuffer, User](
        1,
        1,
        classOf[ByteBuffer],
        HashType.MURMUR3_32,
        "location.countryId",
        SortedBucketIO.DEFAULT_FILENAME_PREFIX,
        classOf[User]
      )
    }
  }
}
