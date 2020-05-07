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

package com.spotify.scio.smb

import java.nio.file.{Files, Path}

import com.spotify.scio.ScioContext
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.beam.sdk.extensions.smb.SortedBucketSource.TargetParallelism
import org.apache.beam.sdk.extensions.smb.{AvroSortedBucketIO, SortedBucketIO}
import org.apache.beam.sdk.values.TupleTag
import org.apache.commons.io.FileUtils
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.JavaConverters._
import scala.util.Random

/**
 * Asserts that SMB join/cogroup operations have parity with the vanilla Scio join/cogroup.
 */
class SortMergeBucketParityIT extends AnyFlatSpec with Matchers {
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

  private val keyFn: GenericRecord => Integer = _.get("key").toString.toInt

  implicit private val coder: Coder[GenericRecord] = Coder.avroGenericRecordCoder(schema)

  "sortMergeCoGroup" should "have parity with a 2-way CoGroup" in withNumSources(2) { inputs =>
    compareResults(
      _.sortMergeCoGroup(
        classOf[Integer],
        mkRead(inputs(0)),
        mkRead(inputs(1)),
        TargetParallelism.min()
      )
    ) { sc =>
      val (avroA, avroB) = (
        sc.avroFile(s"${inputs(0)}/*.avro", schema),
        sc.avroFile(s"${inputs(1)}/*.avro", schema)
      )

      avroA.keyBy(keyFn).cogroup(avroB.keyBy(keyFn))
    }
  }

  it should "have parity with a 2-way CoGroup across multiple input partitions" in
    withNumSources(4) { inputs =>
      compareResults(
        _.sortMergeCoGroup(
          classOf[Integer],
          AvroSortedBucketIO
            .read(new TupleTag[GenericRecord]("lhs"), schema)
            .from(inputs(0).toString, inputs(1).toString),
          AvroSortedBucketIO
            .read(new TupleTag[GenericRecord]("rhs"), schema)
            .from(inputs(2).toString, inputs(3).toString),
          TargetParallelism.max()
        )
      ) { sc =>
        val (lhs, rhs) = (
          SCollection.unionAll(
            List(
              sc.avroFile(s"${inputs(0)}/*.avro", schema),
              sc.avroFile(s"${inputs(1)}/*.avro", schema)
            )
          ),
          SCollection.unionAll(
            List(
              sc.avroFile(s"${inputs(2)}/*.avro", schema),
              sc.avroFile(s"${inputs(3)}/*.avro", schema)
            )
          )
        )

        lhs.keyBy(keyFn).cogroup(rhs.keyBy(keyFn))
      }
    }

  it should "have parity with a 3-way CoGroup" in withNumSources(3) { inputs =>
    compareResults(
      _.sortMergeCoGroup(
        classOf[Integer],
        mkRead(inputs(0)),
        mkRead(inputs(1)),
        mkRead(inputs(2)),
        TargetParallelism.min()
      )
    ) { sc =>
      val (avroA, avroB, avroC) = (
        sc.avroFile(s"${inputs(0)}/*.avro", schema),
        sc.avroFile(s"${inputs(1)}/*.avro", schema),
        sc.avroFile(s"${inputs(2)}/*.avro", schema)
      )

      avroA.keyBy(keyFn).cogroup(avroB.keyBy(keyFn), avroC.keyBy(keyFn))
    }
  }

  it should "have parity with a 4-way CoGroup" in withNumSources(4) { inputs =>
    compareResults(
      _.sortMergeCoGroup(
        classOf[Integer],
        mkRead(inputs(0)),
        mkRead(inputs(1)),
        mkRead(inputs(2)),
        mkRead(inputs(3)),
        TargetParallelism.max()
      )
    ) { sc =>
      val (avroA, avroB, avroC, avroD) = (
        sc.avroFile(s"${inputs(0)}/*.avro", schema),
        sc.avroFile(s"${inputs(1)}/*.avro", schema),
        sc.avroFile(s"${inputs(2)}/*.avro", schema),
        sc.avroFile(s"${inputs(3)}/*.avro", schema)
      )

      avroA.keyBy(keyFn).cogroup(avroB.keyBy(keyFn), avroC.keyBy(keyFn), avroD.keyBy(keyFn))
    }
  }

  "sortMergeGroupByKey" should "have parity with Scio's groupBy" in withNumSources(1) { inputs =>
    compareResults(
      _.sortMergeGroupByKey(classOf[Integer], mkRead(inputs(0)))
    )(sc => sc.avroFile(s"${inputs(0)}/*.avro", schema).groupBy(keyFn))
  }

  "sortMergeJoin" should "have parity with a 2-way Join" in withNumSources(2) { inputs =>
    compareResults(
      _.sortMergeJoin(classOf[Integer], mkRead(inputs(0)), mkRead(inputs(1)))
    ) { sc =>
      val (avroA, avroB) = (
        sc.avroFile(s"${inputs(0)}/*.avro", schema),
        sc.avroFile(s"${inputs(1)}/*.avro", schema)
      )

      avroA.keyBy(keyFn).join(avroB.keyBy(keyFn))
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
