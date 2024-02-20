package com.spotify.scio.smb

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.testing.PipelineTestUtils
import org.apache.beam.sdk.coders.VarIntCoder
import org.apache.beam.sdk.extensions.smb.SortedBucketIO.ComparableKeyBytes
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.BucketedInput
import org.apache.beam.sdk.extensions.smb.SortedBucketTransform.TransformFn
import org.apache.beam.sdk.extensions.smb.{AvroBucketMetadata, AvroFileOperations, AvroSortedBucketIO, BucketMetadata, SortedBucketIO, SortedBucketSource, SortedBucketTransform, TargetParallelism}
import org.apache.beam.sdk.io.LocalResources
import org.apache.beam.sdk.values.TupleTag
import org.scalatest.BeforeAndAfterAll
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

class SmbFlinkIT extends AnyFlatSpec with Matchers with PipelineTestUtils with BeforeAndAfterAll {
  import SmbFlinkIT._

  val tempFolder = Files.createTempDirectory("smb").toFile
  tempFolder.deleteOnExit()

  val writeOutput = new File(tempFolder, "writeOutput").getAbsolutePath
  val tempDir = new File(tempFolder, "tmp").getAbsolutePath

  override def beforeAll(): Unit = {
    val (sc, _) = ContextAndArgs(Array())
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

  "AvroFileOperations" should "be serializable via Flink" in {
    import org.apache.flink.configuration.{Configuration => FlinkConfiguration}
    import org.apache.flink.util.InstantiationUtil
    import scala.jdk.CollectionConverters._

    val conf = new FlinkConfiguration()
    val tfxOutput = new File(tempFolder, "tfxOuptut").getAbsolutePath

    val transformFn: TransformFn[Integer, Account] = {
      case (keyGroup, outputConsumer) =>
        keyGroup.getValue.getAll(new TupleTag[Account]("account")).forEach(outputConsumer.accept(_))
    }

    val bucketedInputs: java.util.List[BucketedInput[_]] = List(AvroSortedBucketIO
      .read(new TupleTag[Account]("account"), classOf[Account])
      .from(writeOutput)
      .toBucketedInput(SortedBucketSource.Keying.PRIMARY)).asJava.asInstanceOf[java.util.List[BucketedInput[_]]]

    val sourceInputFormat =
      new SortedBucketTransform[Integer, Account](
        bucketedInputs,
        ComparableKeyBytes.keyFnPrimary(VarIntCoder.of()),
        new SortedBucketIO.PrimaryKeyComparator(),
        TargetParallelism.max(),
        transformFn,
        null,
        LocalResources.fromString(tfxOutput, true),
        LocalResources.fromString(tempDir, true),
        null,
        (numBuckets: Int, numShards: Int, hashType: BucketMetadata.HashType) => new AvroBucketMetadata(
          numBuckets, numShards, classOf[Integer], "id", null, null, hashType, "bucket", Account.getClassSchema
        ),
        AvroFileOperations.of(new SpecificRecordDatumFactory(classOf[Account]), Account.getClassSchema),
        ".avro",
        "bucket-"
      )

    InstantiationUtil.writeObjectToConfig(sourceInputFormat, conf, "someKey")

    noException should be thrownBy {
      val roundTrip: SortedBucketTransform[Integer, Account] = InstantiationUtil.readObjectFromConfig(conf, "someKey", this.getClass.getClassLoader)
      roundTrip.getName should equal("SortedBucketTransform")
    }
  }
}
