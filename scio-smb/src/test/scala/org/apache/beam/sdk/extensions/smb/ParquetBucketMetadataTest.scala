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
import org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser
import org.apache.beam.sdk.transforms.display.DisplayData
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.ByteArrayOutputStream
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

    val idMeta = new ParquetBucketMetadata[Long, ByteBuffer, GenericRecord](
      1,
      1,
      classOf[Long],
      "id",
      classOf[ByteBuffer],
      "location.countryId",
      HashType.MURMUR3_32,
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      RECORD_SCHEMA
    )
    idMeta.extractKeyPrimary(user) shouldBe 10L
    idMeta.extractKeySecondary(user) shouldBe countryId

    val countryIdMeta = new ParquetBucketMetadata[ByteBuffer, Long, GenericRecord](
      1,
      1,
      classOf[ByteBuffer],
      "location.countryId",
      classOf[Long],
      "id",
      HashType.MURMUR3_32,
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      RECORD_SCHEMA
    )
    countryIdMeta.extractKeyPrimary(user) shouldBe countryId
    countryIdMeta.extractKeySecondary(user) shouldBe 10L

    val postalCodeMeta = new ParquetBucketMetadata[ByteBuffer, Void, GenericRecord](
      1,
      1,
      classOf[ByteBuffer],
      "location.postalCode",
      null,
      null,
      HashType.MURMUR3_32,
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      RECORD_SCHEMA
    )
    postalCodeMeta.extractKeyPrimary(user) shouldBe postalCode

    val suffixMeta = new ParquetBucketMetadata[String, Void, GenericRecord](
      1,
      1,
      classOf[String],
      "suffix",
      null,
      null,
      HashType.MURMUR3_32,
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      RECORD_SCHEMA
    )
    suffixMeta.extractKeyPrimary(user) shouldBe "Jr"
  }

  it should "work with specific records" in {
    val user = new AvroGeneratedUser("foo", 50, "green")

    val colorMeta = new ParquetBucketMetadata[String, Void, AvroGeneratedUser](
      1,
      1,
      classOf[String],
      "favorite_color",
      null,
      null,
      HashType.MURMUR3_32,
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )
    colorMeta.extractKeyPrimary(user) shouldBe "green"

    val numberMeta = new ParquetBucketMetadata[Int, Void, AvroGeneratedUser](
      1,
      1,
      classOf[Int],
      "favorite_number",
      null,
      null,
      HashType.MURMUR3_32,
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )
    numberMeta.extractKeyPrimary(user) shouldBe 50
  }

  it should "work with case classes" in {
    import ParquetBucketMetadataTest._
    val user = User(10L, "Johnny", Location(12345, "US"))

    val idMeta = new ParquetBucketMetadata[Long, Void, User](
      1,
      1,
      classOf[Long],
      "id",
      null,
      null,
      HashType.MURMUR3_32,
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[User]
    )
    idMeta.extractKeyPrimary(user) shouldBe 10L

    val countryIdMeta = new ParquetBucketMetadata[String, Void, User](
      1,
      1,
      classOf[String],
      "location.countryId",
      null,
      null,
      HashType.MURMUR3_32,
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[User]
    )
    countryIdMeta.extractKeyPrimary(user) shouldBe "US"

    val postalCodeMeta = new ParquetBucketMetadata[Int, Void, User](
      1,
      1,
      classOf[Int],
      "location.postalCode",
      null,
      null,
      HashType.MURMUR3_32,
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[User]
    )
    postalCodeMeta.extractKeyPrimary(user) shouldBe 12345
  }

  it should "round trip" in {
    val metadata = new ParquetBucketMetadata[String, Void, AvroGeneratedUser](
      1,
      1,
      1,
      classOf[String],
      "favorite_color",
      null,
      null,
      BucketMetadata.serializeHashType(HashType.MURMUR3_32),
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
      "favorite_color",
      null,
      null,
      HashType.MURMUR3_32,
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
      "favorite_color",
      null,
      null,
      HashType.MURMUR3_32,
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )
    val displayData = DisplayData.from(metadata).asMap().asScala.map { kv =>
      kv._1.getKey -> kv._2.getValue
    }
    displayData.get("numBuckets") shouldBe Some(2)
    displayData.get("numShards") shouldBe Some(1)
    displayData.get("version") shouldBe Some(BucketMetadata.CURRENT_VERSION)
    displayData.get("keyFieldPrimary") shouldBe Some("favorite_color")
    displayData.get("keyClassPrimary") shouldBe Some(classOf[String].getName)
    displayData.get("hashType") shouldBe Some(HashType.MURMUR3_32.toString)
    displayData.get("keyCoderPrimary") shouldBe Some(classOf[StringUtf8Coder].getName)
  }

  it should "check source compatibility" in {
    val metadata1 = new ParquetBucketMetadata[String, Void, AvroGeneratedUser](
      2,
      1,
      classOf[String],
      "name",
      null,
      null,
      HashType.MURMUR3_32,
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )
    val metadata2 = new ParquetBucketMetadata[String, Void, AvroGeneratedUser](
      2,
      1,
      classOf[String],
      "favorite_color",
      null,
      null,
      HashType.MURMUR3_32,
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )
    val metadata3 = new ParquetBucketMetadata[String, Void, AvroGeneratedUser](
      4,
      1,
      classOf[String],
      "favorite_color",
      null,
      null,
      HashType.MURMUR3_32,
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )
    val metadata4 = new ParquetBucketMetadata[Int, Void, AvroGeneratedUser](
      4,
      1,
      classOf[Int],
      "favorite_number",
      null,
      null,
      HashType.MURMUR3_32,
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )

    metadata1.isPartitionCompatibleForPrimaryKey(metadata2) shouldBe false
    metadata2.isPartitionCompatibleForPrimaryKey(metadata3) shouldBe true
    metadata3.isPartitionCompatibleForPrimaryKey(metadata4) shouldBe false
  }

  it should "check key type" in {
    an[IllegalArgumentException] shouldBe thrownBy {
      new ParquetBucketMetadata[String, Void, GenericRecord](
        1,
        1,
        classOf[String],
        "location.countryId",
        null,
        null,
        HashType.MURMUR3_32,
        SortedBucketIO.DEFAULT_FILENAME_PREFIX,
        RECORD_SCHEMA
      )
    }

    an[IllegalArgumentException] shouldBe thrownBy {
      import ParquetBucketMetadataTest._
      new ParquetBucketMetadata[ByteBuffer, Void, User](
        1,
        1,
        classOf[ByteBuffer],
        "location.countryId",
        null,
        null,
        HashType.MURMUR3_32,
        SortedBucketIO.DEFAULT_FILENAME_PREFIX,
        classOf[User]
      )
    }
  }

  it should "not write null secondary keys" in {
    val metadata = new ParquetBucketMetadata[String, Void, AvroGeneratedUser](
      2,
      1,
      classOf[String],
      "name",
      null,
      null,
      HashType.MURMUR3_32,
      SortedBucketIO.DEFAULT_FILENAME_PREFIX,
      classOf[AvroGeneratedUser]
    )

    val os = new ByteArrayOutputStream
    BucketMetadata.to(metadata, os)

    os.toString should not include "keyFieldSecondary"
    BucketMetadata
      .from(os.toString)
      .asInstanceOf[ParquetBucketMetadata[String, Void, GenericRecord]]
      .getKeyClassSecondary should be(null)
  }
}
