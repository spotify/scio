/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.bigquery

import com.google.api.services.bigquery.model.{Table => GTAble, TableReference}
import com.google.cloud.bigquery.storage.v1._
import com.google.protobuf.ByteString
import com.spotify.scio.avro._
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.testing._
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryServices.StorageClient
import org.apache.beam.sdk.io.gcp.testing.FakeBigQueryServices.FakeBigQueryServerStream
import org.apache.beam.sdk.io.gcp.testing.{FakeBigQueryServices, FakeDatasetService, FakeJobService}
import org.apache.beam.sdk.io.gcp.{bigquery => beam}
import org.apache.beam.sdk.transforms.display.DisplayData
import org.apache.beam.sdk.util.CoderUtils
import org.scalatest.BeforeAndAfterAll

import scala.jdk.CollectionConverters._

object BigQueryIOTest {
  @BigQueryType.toTable
  case class BQRecord(i: Int, s: String, r: List[String])

  // BQ Write transform display id data for tableDescription
  private val TableDescriptionId = DisplayData.Identifier.of(
    DisplayData.Path.root(),
    classOf[beam.BigQueryIO.Write[_]],
    "tableDescription"
  )

  final class FakeStorageClient(data: Seq[BQRecord]) extends StorageClient with Serializable {

    @transient
    private lazy val bqt: BigQueryType[BQRecord] = BigQueryType[BQRecord]

    override def createReadSession(request: CreateReadSessionRequest): ReadSession = ReadSession
      .newBuilder()
      .setName("session")
      .setAvroSchema(AvroSchema.newBuilder().setSchema(bqt.avroSchema.toString()))
      .addStreams(ReadStream.newBuilder().setName("stream"))
      .setDataFormat(DataFormat.AVRO)
      .build()

    override def readRows(
      request: ReadRowsRequest
    ): BigQueryServices.BigQueryServerStream[ReadRowsResponse] = {
      val bcoder = CoderMaterializer.beamWithDefault(avroGenericRecordCoder(bqt.avroSchema))
      val bytes = data
        .foldLeft(ByteString.newOutput()) { (bs, r) =>
          bs.write(CoderUtils.encodeToByteArray(bcoder, bqt.toAvro(r)))
          bs
        }
        .toByteString

      new FakeBigQueryServerStream(
        List(
          ReadRowsResponse
            .newBuilder()
            .setAvroRows(AvroRows.newBuilder().setSerializedBinaryRows(bytes).build())
            .setRowCount(data.size.toLong)
            .build()
        ).asJava
      )
    }

    override def readRows(
      request: ReadRowsRequest,
      fullTableId: String
    ): BigQueryServices.BigQueryServerStream[ReadRowsResponse] = readRows(request)

    override def splitReadStream(request: SplitReadStreamRequest): SplitReadStreamResponse = ???
    override def splitReadStream(
      request: SplitReadStreamRequest,
      fullTableId: String
    ): SplitReadStreamResponse = ???

    override def close(): Unit = ()
  }
}

final class BigQueryIOTest extends ScioIOSpec with BeforeAndAfterAll {
  import BigQueryIOTest._

  override def beforeAll(): Unit =
    FakeDatasetService.setUp()

  "BigQueryIO" should "apply config override" in {
    val name = "saveAsBigQueryTable"
    val desc = "table-description"
    val sc = ScioContext()
    implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder
    val io = BigQueryIO[GenericRecord](
      Table("project:dataset.out_table")
    )
    val params = BigQueryIO.WriteParam[GenericRecord](
      format = BigQueryIO.Format.Avro(),
      createDisposition = CreateDisposition.CREATE_NEVER,
      configOverride = _.withTableDescription(desc)
    )
    sc.empty[GenericRecord]()
      .withName(name)
      .write(io)(params)

    val finder = new TransformFinder(new EqualNamePTransformMatcher(name))
    sc.pipeline.traverseTopologically(finder)
    val transform = finder.result().head
    val displayData = DisplayData.from(transform).asMap().asScala
    displayData should contain key TableDescriptionId
    displayData(TableDescriptionId).getValue shouldBe desc
  }

  it should "work with TableRow" in {
    val xs = (1 to 100).map(x => TableRow("x" -> x.toString))
    testJobTest(xs, in = "project:dataset.in_table", out = "project:dataset.out_table")(
      BigQueryIO(_)
    )((sc, s) => sc.bigQueryTable(Table(s)))((coll, s) => coll.saveAsBigQueryTable(Table(s)))
  }

  it should "read the same input table with different predicate and projections using bigQueryStorage" in {
    JobTest[JobWithDuplicateInput.type]
      .args("--input=table.in")
      .input(
        BigQueryIO[TableRow](Table("table.in", List("a"), "a > 0")),
        (1 to 3).map(x => TableRow("x" -> x.toString))
      )
      .input(
        BigQueryIO[TableRow](Table("table.in", List("b"), "b > 0")),
        (1 to 3).map(x => TableRow("x" -> x.toString))
      )
      .run()
  }

  it should "read the same input table with different predicate and projections using typedBigQueryStorage" in {
    JobTest[TypedJobWithDuplicateInput.type]
      .args("--input=table.in")
      .input(
        BigQueryIO[BQRecord](Table("table.in", List("a"), "a > 0")),
        (1 to 3).map(x => BQRecord(x, x.toString, (1 to x).map(_.toString).toList))
      )
      .input(
        BigQueryIO[BQRecord](Table("table.in", List("b"), "b > 0")),
        (1 to 3).map(x => BQRecord(x, x.toString, (1 to x).map(_.toString).toList))
      )
      .run()
  }

  it should "propagate errors if handler is set" in {
    val ref = new TableReference()
      .setProjectId("project")
      .setDatasetId("dataset")
      .setTableId("table")

    val bqt = BigQueryType[BQRecord]
    val data = Seq(
      BQRecord(1, "a", List("1")),
      BQRecord(2, "b", List("2"))
    )

    val failingFormat = BigQueryIO.Format.Avro(
      bqt.fromAvro.andThen(r => if (r.i % 2 == 0) throw new Exception("fail") else r),
      bqt.toAvro
    )

    val fakeDatasetService = new FakeDatasetService()
    val fakeJobService = new FakeJobService()
    val fakeStorageClient = new FakeStorageClient(data)

    val fakeBqServices = new FakeBigQueryServices()
      .withDatasetService(fakeDatasetService)
      .withJobService(fakeJobService)
      .withStorageClient(fakeStorageClient)

    try {
      fakeDatasetService.createDataset(ref.getProjectId, ref.getDatasetId, "US", "desc", -1)
      fakeDatasetService.createTable(new GTAble().setTableReference(ref).setSchema(bqt.schema))
      fakeDatasetService.insertAll(ref, data.map(bqt.toTableRow).asJava, null)

      runWithRealContext() { sc =>
        val table = Table(ref)
        val errors = sc.errorSink()
        sc.bigQueryStorageFormat[BQRecord](
          table,
          failingFormat,
          errorHandler = errors.handler,
          configOverride = _.withTestServices(fakeBqServices)
        )

        val recordWithFailure = errors.sink.map { br =>
          val record = br.getRecord.getHumanReadableJsonRecord
          val desc = br.getFailure.getDescription
          val exception = br.getFailure.getException
          (record, desc, exception)
        }
        recordWithFailure should containSingleValue(
          (
            """{"i": 2, "s": "b", "r": ["2"]}""",
            "Unable to parse record reading from BigQuery",
            "java.lang.Exception: fail"
          )
        )
      }
    } finally {
      fakeDatasetService.close()
      fakeJobService.close()
    }
  }

  "TableRowJsonIO" should "work" in {
    val xs = (1 to 100).map(x => TableRow("x" -> x.toString))
    testTap(xs)(_.saveAsTableRowJsonFile(_))(".json")
    testJobTest(xs)(TableRowJsonIO(_))(_.tableRowJsonFile(_))(_.saveAsTableRowJsonFile(_))
  }

}

object JobWithDuplicateInput {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.bigQueryStorage(Table(args("input"), List("a"), "a > 0"))
    sc.bigQueryStorage(Table(args("input"), List("b"), "b > 0"))
    sc.run()
    ()
  }
}

object TypedJobWithDuplicateInput {
  import BigQueryIOTest._

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.typedBigQueryStorage[BQRecord](Table(args("input"), List("a"), "a > 0"))
    sc.typedBigQueryStorage[BQRecord](Table(args("input"), List("b"), "b > 0"))
    sc.run()
    ()
  }
}
