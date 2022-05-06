package com.spotify.scio.extra.bigquery

import java.{util => ju}

import com.google.protobuf.ByteString
import com.spotify.scio.avro.types.AvroType
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.bigquery.Table
import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.coders._
import com.spotify.scio.ContextAndArgs
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryAvroUtilsWrapper
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object BigQueryIT {
  @AvroType.fromSchema("""{
      | "type":"record",
      | "name":"Account",
      | "namespace":"com.spotify.scio.avro",
      | "doc":"Record for an account",
      | "fields":[
      |   {"name":"id","type":"long"},
      |   {"name":"type","type":"string"},
      |   {"name":"name","type":"string"},
      |   {"name":"amount","type":"double"},
      |   {"name":"secret","type":"bytes"}]}
    """.stripMargin)
  class Account

  implicit def genericCoder = Coder.avroGenericRecordCoder(Account.schema)

}

final class BigQueryIT extends AnyFlatSpec with Matchers {
  import BigQueryIT._

  it should "save avro to BigQuery" in {
    val args = Array(
      "--project=data-integration-test",
      "--tempLocation=gs://data-integration-test-eu/temp"
    )
    val (sc, _) = ContextAndArgs(args)
    val prefix = ju.UUID.randomUUID().toString.replaceAll("-", "")
    val table = Table.Spec(s"data-integration-test:bigquery_avro_it.${prefix}_accounts")

    val data: Seq[GenericRecord] = (1 to 100).map { i =>
      Account.toGenericRecord(
        Account(i.toLong, "checking", s"account$i", i.toDouble, ByteString.copyFromUtf8("%20cフーバー"))
      )
    }

    val tap = sc
      .parallelize(data)
      .saveAvroAsBigQuery(
        table.ref,
        Account.schema,
        writeDisposition = WriteDisposition.WRITE_EMPTY,
        createDisposition = CreateDisposition.CREATE_IF_NEEDED
      )

    val result = sc.run().waitUntilDone()

    val ts = BigQuery.defaultInstance().tables.schema(table.ref)
    val expected: Seq[TableRow] = data.map { gr =>
      BigQueryAvroUtilsWrapper.convertGenericRecordToTableRow(gr, ts)
    }

    result.tap(tap).value.toSet shouldEqual expected.toSet
  }

}
