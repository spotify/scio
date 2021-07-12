package com.spotify.scio.smb.util

import com.spotify.scio.ScioContext
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.smb.toSortMergeBucketSCollection
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.MultiJoin
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.beam.sdk.extensions.smb.{AvroSortedBucketIO, SortedBucketIO, TargetParallelism}
import org.apache.beam.sdk.values.TupleTag
import org.apache.commons.io.FileUtils
import org.scalatest.Assertion

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.Random

class SMBMultiJoinTest extends PipelineSpec {

  private val schema = Schema.createRecord(
    "User",
    "",
    "com.spotify.scio.smb",
    false,
    List(
      new Field("key", Schema.create(Schema.Type.INT), "", -1),
      new Field("value", Schema.create(Schema.Type.STRING), "", "")
    ).asJava
  )

  implicit private val coder: Coder[GenericRecord] = Coder.avroGenericRecordCoder(schema)

  private val keyFn: GenericRecord => Integer = _.get("key").toString.toInt

  "SMBMultiJoin" should "have parity with a 5-way CoGroup" in withNumSources(5) { inputs =>
    compareResults(
      com.spotify.scio.smb.util
        .SMBMultiJoin(_)
        .sortMergeCoGroup(
          classOf[Integer],
          mkRead(inputs(0)),
          mkRead(inputs(1)),
          mkRead(inputs(2)),
          mkRead(inputs(3)),
          mkRead(inputs(4)),
          TargetParallelism.auto()
        )
    ) { sc =>
      val (avroA, avroB, avroC, avroD, avroE) = (
        sc.avroFile(s"${inputs(0)}/*.avro", schema),
        sc.avroFile(s"${inputs(1)}/*.avro", schema),
        sc.avroFile(s"${inputs(2)}/*.avro", schema),
        sc.avroFile(s"${inputs(3)}/*.avro", schema),
        sc.avroFile(s"${inputs(4)}/*.avro", schema)
      )

      MultiJoin.cogroup(
        avroA.keyBy(keyFn),
        avroB.keyBy(keyFn),
        avroC.keyBy(keyFn),
        avroD.keyBy(keyFn),
        avroE.keyBy(keyFn)
      )
    }
  }

  // Write randomly generated Avro records in SMB fashion to `numSources` destinations
  // in the local file system
  private def withNumSources(numSources: Int)(
    testFn: Map[Int, Path] => Any
  ): Unit = {
    val tempFolder = Files.createTempDirectory("smb")
    val sc = ScioContext()

    val outputPaths = (0 until numSources).map { n =>
      val data = (0 to Random.nextInt(100)).map { i =>
        val gr: GenericRecord = new GenericData.Record(schema)
        gr.put("key", i)
        gr.put("value", s"v$i")
        gr
      }

      val outputPath = tempFolder.resolve(s"source$n")

      sc.parallelize(data)
        .saveAsSortedBucket(
          AvroSortedBucketIO
            .write(classOf[Integer], "key", schema)
            .to(outputPath.toString)
            .withNumBuckets(Math.pow(2.0, 1.0 * n).toInt)
            .withNumShards(2)
        )

      n -> outputPath
    }.toMap

    sc.run().waitUntilDone()

    try {
      testFn(outputPaths)
    } finally {
      FileUtils.deleteDirectory(tempFolder.toFile)
    }
  }

  private def mkRead(path: Path): SortedBucketIO.Read[GenericRecord] =
    AvroSortedBucketIO
      .read(new TupleTag[GenericRecord](path.toString), schema)
      .from(path.toString)

  private def compareResults[T: Coder](
    smbOp: ScioContext => SCollection[T]
  )(baselineOp: ScioContext => SCollection[T]): Assertion = {
    val sc = ScioContext()
    val smbTap = smbOp(sc).materialize
    val baselineTap = baselineOp(sc).materialize

    val closedContext = sc.run().waitUntilDone()

    val smbResults = closedContext.tap(smbTap).value.toSeq
    val baselineResults = closedContext.tap(baselineTap).value.toSeq

    smbResults should contain theSameElementsAs baselineResults
  }
}
