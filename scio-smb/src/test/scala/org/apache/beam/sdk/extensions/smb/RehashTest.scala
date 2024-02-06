package org.apache.beam.sdk.extensions.smb

import com.spotify.scio.ScioContext
import com.spotify.scio.avro._
import com.spotify.scio.smb._
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.extensions.avro.io.AvroGeneratedUser
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType
import org.apache.beam.sdk.extensions.smb.SortedBucketIO.ComparableKeyBytes
import org.apache.beam.sdk.io.{FileSystems, LocalResources}
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.values.{KV, PCollection, TupleTag}
import org.scalatest.BeforeAndAfterAll

import java.nio.channels.Channels
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._

object RehashTest {
  def createTempDir(prefix: String): Path = {
    val path = Files.createTempDirectory(prefix)
    path.toFile.deleteOnExit()
    path
  }

  val records = (1 to 1000)
    .map(i =>
      AvroGeneratedUser
        .newBuilder()
        .setName(s"user_$i")
        .setFavoriteColor(s"color_$i")
        .setFavoriteNumber(i)
        .build()
    )

  val inputDir = createTempDir("input")
}

class RehashTest extends PipelineSpec with BeforeAndAfterAll {
  import RehashTest._

  override def beforeAll(): Unit = {
    val sc = ScioContext()
    sc
      .parallelize(records)
      .saveAsSortedBucket(
        ParquetAvroSortedBucketIO
          .write(classOf[String], "name", classOf[AvroGeneratedUser])
          .to(inputDir.toString)
          .withNumBuckets(8)
          .withFilenamePrefix("part")
          .withHashType(HashType.MURMUR3_32)
      )
    sc.run()
  }

  // Result: pass
  "Rehash" should "convert from Murmur3_32 to Murmur3_128" in {
    testRehash(HashType.MURMUR3_128)
  }

  // Result: failure
  it should "convert from Murmur3_32 to ICEBERG" in {
    testRehash(HashType.ICEBERG)
  }

  def testRehash(newHashType: HashType): Unit = {
    val outputDir = createTempDir(s"rehashOut-${newHashType}")
    val tmpDir = createTempDir(s"tmp-${newHashType}")
    val fileOperations = ParquetAvroFileOperations.of(classOf[AvroGeneratedUser])

    val sc = ScioContext()
    val rehash = new Rehash[String, AvroGeneratedUser](
      ParquetAvroSortedBucketIO
        .read(new TupleTag[AvroGeneratedUser]("input"), classOf[AvroGeneratedUser])
        .from(inputDir.toString)
        .toBucketedInput(SortedBucketSource.Keying.PRIMARY),
      LocalResources.fromFile(outputDir.toFile, true),
      LocalResources.fromFile(tmpDir.toFile, true),
      newHashType,
      ".parquet",
      fileOperations
    )

    val writeResult = sc
      .wrap(
        sc.pipeline
          .apply("Rehash", rehash)
          .expand()
          .get(new TupleTag("WrittenFiles"))
          .asInstanceOf[PCollection[KV[BucketShardId, ResourceId]]]
      )
      .materialize

    val pipelineResult = sc.run().waitUntilDone()

    // Assert that metadata uses the new hash type
    val metadata = new ParquetBucketMetadata[String, Void, AvroGeneratedUser](
      8,
      1,
      classOf[String],
      "name",
      null,
      null,
      newHashType,
      "part",
      classOf[AvroGeneratedUser]
    )

    BucketMetadata
      .from(
        Channels.newInputStream(
          FileSystems.open(
            LocalResources.fromFile(outputDir.resolve("metadata.json").toFile, false)
          )
        )
      )
      .toString should equal(metadata.toString)

    // Assert that all records have been written
    val writtenFiles = writeResult.get(pipelineResult).value.toSeq

    // Assert that each file contains correctly sorted and hashed records
    val comparator = new SortedBucketIO.PrimaryKeyComparator

    val writtenRecords = writtenFiles.flatMap { kv =>
      val bucketShardId = kv.getKey
      bucketShardId.getShardId should equal(0)

      val expectedFilename = if (bucketShardId.isNullKeyBucket) {
        "part-null-keys"
      } else {
        s"part-${"%05d".format(bucketShardId.getBucketId)}-of-${"%05d".format(8)}"
      }

      kv.getValue should equal(
        LocalResources.fromFile(outputDir.resolve(s"$expectedFilename.parquet").toFile, false)
      )

      var lastRecordKey: ComparableKeyBytes = null
      fileOperations.iterator(kv.getValue).asScala.map { record =>
        // Check bucket ID matches destination
        val keyBytes = metadata.primaryComparableKeyBytes(record)
        val hashedBucketId = metadata.getBucketId(keyBytes.primary)
        hashedBucketId should equal(bucketShardId.getBucketId)

        // Check that records are in sorted order
        if (lastRecordKey == null) {
          lastRecordKey = keyBytes
        } else {
          comparator.compare(lastRecordKey, keyBytes) should be <= 0
          lastRecordKey = keyBytes
        }

        record
      }
    }

    writtenRecords should contain theSameElementsAs records

    // Assert that counters have been incremented
    pipelineResult
      .counter(
        Metrics.counter(classOf[Rehash[String, AvroGeneratedUser]], "RecordsRead")
      )
      .committed
      .get should equal(1000)
    pipelineResult
      .counter(
        Metrics.counter(classOf[Rehash[String, AvroGeneratedUser]], "RecordsWritten")
      )
      .committed
      .get should equal(1000)
  }
}
