/*
 * Copyright 2026 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.avro.reflecttest

import com.spotify.scio.avro._
import com.spotify.scio.coders.{Coder, CoderMaterializer, MaterializedCoder}
import com.spotify.scio.testing.CoderAssertions._
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.AvroClassTrust
import org.apache.avro.{Conversion, Schema}
import org.apache.avro.reflect.{Nullable, ReflectData}
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory
import org.apache.beam.sdk.util.CoderUtils

import java.math.BigDecimal
import scala.annotation.meta.field
import scala.jdk.CollectionConverters._

// reflect records, in a package of their own so the test can trust it on avro 1.12.1+
case class Amount(var value: BigDecimal) {
  def this() = this(null)
}

case class Ledger(
  var name: String,
  var total: BigDecimal,
  @(Nullable @field) var discount: BigDecimal,
  var items: java.util.List[BigDecimal],
  var byKey: java.util.Map[String, BigDecimal],
  var nested: Amount
) {
  def this() = this(null, null, null, null, null, null)
}

class ReflectRecordDatumFactoryTest extends PipelineSpec {

  AvroClassTrust.trustPackages(getClass.getPackage.getName)

  private val ledger = Ledger(
    "l",
    new BigDecimal("12.50"),
    new BigDecimal("-0.001"),
    List(new BigDecimal("1"), new BigDecimal("1.000")).asJava,
    Map("k" -> new BigDecimal("1E+3")).asJava,
    Amount(new BigDecimal("123456789012345678901234567890.123456789"))
  )

  private val reflectSchema = ReflectData.get().getSchema(classOf[Ledger])

  // avro 1.12.1+ maps BigDecimal to the big-decimal logical type, older versions to a string
  private val isBigDecimalLogicalType =
    reflectSchema.getField("total").schema().getProp("logicalType") == "big-decimal"

  private def encodeDecode[T](coder: org.apache.beam.sdk.coders.Coder[T], value: T): T =
    CoderUtils.decodeFromByteArray(coder, CoderUtils.encodeToByteArray(coder, value))

  "ReflectRecordDatumFactory" should "roundtrip BigDecimal fields with the reflect schema" in {
    val coder = AvroCoder.of(new ReflectRecordDatumFactory(classOf[Ledger]), reflectSchema)
    val decoded = encodeDecode(coder, ledger)
    decoded shouldBe ledger
    // BigDecimal equality includes the scale
    decoded.items.asScala.map(_.scale) shouldBe List(0, 3)
  }

  it should "roundtrip a null nullable BigDecimal" in {
    val coder = AvroCoder.of(new ReflectRecordDatumFactory(classOf[Ledger]), reflectSchema)
    val withNull = ledger.copy(discount = null)
    encodeDecode(coder, withNull) shouldBe withNull
  }

  it should "register the BigDecimal conversion when avro maps BigDecimal to big-decimal" in {
    assume(isBigDecimalLogicalType, "avro < 1.12.1 maps BigDecimal to a string")
    val logicalType = reflectSchema.getField("total").schema().getLogicalType
    val data = ReflectRecordDatumFactory.reflectData(classOf[Ledger])
    // getConversionFor isn't generic on avro 1.8
    val conversion: Conversion[_] = data.getConversionFor(logicalType)
    Option(conversion).map(_.getConvertedType) shouldBe Some(classOf[BigDecimal])
  }

  it should "fix Beam's ReflectDatumFactory failing on avro 1.12.1+" in {
    assume(isBigDecimalLogicalType, "avro < 1.12.1 maps BigDecimal to a string")
    // what ContoursIO.BigDecimalReflectDatumFactory worked around
    val beamCoder = AvroCoder.of(AvroDatumFactory.reflect(classOf[Ledger]), reflectSchema)
    an[Exception] should be thrownBy encodeDecode(beamCoder, ledger)
  }

  it should "read data written with Beam's reflect coder on avro < 1.12.1" in {
    assume(!isBigDecimalLogicalType, "only avro < 1.12.1 has the string encoding")
    val beamCoder = AvroCoder.reflect(classOf[Ledger])
    val coder = AvroCoder.of(new ReflectRecordDatumFactory(classOf[Ledger]), reflectSchema)
    val bytes = CoderUtils.encodeToByteArray(beamCoder, ledger)
    CoderUtils.decodeFromByteArray(coder, bytes) shouldBe ledger
  }

  it should "be serializable" in {
    val coder = AvroCoder.of(new ReflectRecordDatumFactory(classOf[Ledger]), reflectSchema)
    val copy = org.apache.beam.sdk.util.SerializableUtils.clone(coder)
    encodeDecode(copy, ledger) shouldBe ledger
  }

  "avroReflectCoder" should "roundtrip reflect records with BigDecimal fields" in {
    implicit val coder: Coder[Ledger] = avroReflectCoder[Ledger]
    ledger coderShould roundtrip()
    ledger.copy(discount = null) coderShould roundtrip()
    coderIsSerializable[Ledger]
  }

  it should "use the reflect schema" in {
    val beamCoder = CoderMaterializer.beamWithDefault(avroReflectCoder[Ledger]) match {
      case c: MaterializedCoder[Ledger] => c.bcoder
      case c                            => c
    }
    beamCoder shouldBe an[AvroCoder[_]]
    val schema = beamCoder.asInstanceOf[AvroCoder[Ledger]].getSchema
    schema shouldBe reflectSchema
    if (isBigDecimalLogicalType) {
      schema.getField("total").schema().getType shouldBe Schema.Type.BYTES
    }
  }

  it should "work in a pipeline" in {
    runWithContext { sc =>
      val out = sc
        .parallelize(1 to 10)
        .map(i => ledger.copy(name = i.toString, total = new BigDecimal(s"$i.25")))(
          avroReflectCoder[Ledger]
        )
        .groupBy(_.total.scale)
        .flatMap(_._2)(avroReflectCoder[Ledger])
        .map(_.total)
      out should containInAnyOrder((1 to 10).map(i => new BigDecimal(s"$i.25")))
    }
  }
}
