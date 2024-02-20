package com.spotify.scio.smb

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.testing.PipelineTestUtils
import org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO
import org.apache.beam.sdk.values.TupleTag
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

object SmbFlinkIT {
  def account(id: Int): Account = {
    Account
      .newBuilder()
      .setId(id)
      .setType(s"type${id % 3}")
      .setName(s"account $id")
      .setAmount(id * 10.0)
      .setAccountStatus(AccountStatus.Active)
      .build()
  }
}

class SmbFlinkIT extends AnyFlatSpec with Matchers with PipelineTestUtils {
  import SmbFlinkIT._

  "SMB writes and reads" should "work end to end with Avro types on Flink runner" in {
    val tempFolder = Files.createTempDirectory("smb").toFile
    tempFolder.deleteOnExit()

    val tempDir = new File(tempFolder, "tmp").getAbsolutePath
    val writeOutput = new File(tempFolder, "writeOutput").getAbsolutePath

    // Write
    {
      val (sc, _) = ContextAndArgs(Array("--runner=FlinkRunner", "--parallelism=1"))
      sc.parallelize(1 to 100)
        .map(account)
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "id", classOf[Account])
            .to(writeOutput)
            .withTempDirectory(tempDir)
            .withNumBuckets(2)
            .withNumShards(1)
        )
      sc.run()
    }
    // Read
    {
      val (sc, _) = ContextAndArgs(Array("--runner=FlinkRunner", "--parallelism=1"))
      sc.sortMergeGroupByKey(
        classOf[Integer],
        AvroSortedBucketIO
          .read(new TupleTag[Account], classOf[Account])
          .from(writeOutput)
      ).map(identity)
      sc.run()
    }

    // Transform
    val tfxOutput = new File(tempFolder, "tfxOutput").getAbsolutePath

    {
      val (sc, _) = ContextAndArgs(Array("--runner=FlinkRunner", "--parallelism=1"))

      sc.sortMergeTransform(
        classOf[Integer],
        AvroSortedBucketIO
          .read(new TupleTag[Account], classOf[Account])
          .from(writeOutput)
      ).to(
        AvroSortedBucketIO
          .transformOutput(classOf[Integer], "id", classOf[Account])
          .withTempDirectory(tempDir)
          .to(tfxOutput)
      ).via { case (_, accounts, consumer) =>
        accounts.foreach(consumer.accept)
      }

      sc.run()
    }
  }
}
