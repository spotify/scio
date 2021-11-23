package com.spotify.scio.avro

import com.spotify.scio.ScioContext
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.reflect.io.Directory
import scala.tools.nsc.io.Path

class AvroIOSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // type LogicalDate = org.joda.time.LocalDate
  type LogicalDate = java.time.LocalDate

  def createDate(year: Int, month: Int, day: Int): LogicalDate =
    // new org.joda.time.LocalDate(year, month, day)
    java.time.LocalDate.of(year, month, day)

  val output: Directory = Path("target/specific/").toDirectory

  val dataset = List(
    Test
      .newBuilder()
      .setField(1)
      .setLogicalField(createDate(2021, 2, 3))
      .setNullableLogicalField(null)
      .setReflectField(BigDecimal(1.23).bigDecimal)
      .setNullableReflectField(null)
      .build(),
    Test
      .newBuilder()
      .setField(1)
      .setLogicalField(createDate(2021, 2, 3))
      .setNullableLogicalField(createDate(2024, 5, 6))
      .setReflectField(BigDecimal(1.23).bigDecimal)
      .setNullableReflectField(BigDecimal(45.6).bigDecimal)
      .build()
  )

  override def beforeAll(): Unit =
    output.deleteRecursively()

  "AvroIO" should "support full avro spec when writing" in {
    val ctx = ScioContext()
    val param = AvroIO.WriteParam(
      AvroIO.WriteParam.DefaultNumShards,
      AvroIO.WriteParam.DefaultSuffix,
      AvroIO.WriteParam.DefaultCodec,
      AvroIO.WriteParam.DefaultMetadata,
      AvroIO.WriteParam.DefaultTempDirectory
    )
    ctx.parallelize(dataset).write(SpecificRecordIO[Test](output.path))(param)
    val result = ctx.run()
    result.waitUntilFinish()

    output.list should not be empty
  }

  it should "support full avro spec when reading" in {
    val ctx = ScioContext()
    val files = output / Path("part-*")
    val decoded = ctx.read(SpecificRecordIO[Test](files.path)).materialize
    val result = ctx.run().waitUntilFinish()
    result.tap(decoded).value.toSeq should contain theSameElementsAs dataset
  }

}
