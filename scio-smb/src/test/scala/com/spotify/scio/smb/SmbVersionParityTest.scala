package com.spotify.scio.smb

import com.spotify.scio.avro.{Account, AccountStatus}
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.extensions.smb.AvroSortedBucketIO
import org.apache.beam.sdk.values.TupleTag

import java.nio.file.Files

class SmbVersionParityTest extends PipelineSpec {
  "SortedBucketSource" should "be able to read CharSequence-keyed sources written before 0.14" in {
    val output = Files.createTempDirectory("smb-version-test").toFile
    output.deleteOnExit()

    val accounts = (1 to 10).map { i =>
      Account
        .newBuilder()
        .setId(i)
        .setName(i.toString)
        .setAmount(i.toDouble)
        .setType(s"type$i")
        .setAccountStatus(AccountStatus.Active)
        .build()
    }

    {
      val sc = ScioContext()
      sc.parallelize(accounts)
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[CharSequence], "name", classOf[Account])
            .to(output.getAbsolutePath)
            .withNumBuckets(1)
            .withNumShards(1)
        )
      sc.run()
    }

    // Read data
    val sc = ScioContext()
    val tap = sc
      .sortMergeGroupByKey(
        classOf[String],
        AvroSortedBucketIO
          .read(new TupleTag[Account], classOf[Account])
          .from(output.getAbsolutePath)
      )
      .materialize
    tap
      .get(sc.run().waitUntilDone())
      .flatMap(_._2)
      .value
      .toSeq should contain theSameElementsAs accounts
  }
}
