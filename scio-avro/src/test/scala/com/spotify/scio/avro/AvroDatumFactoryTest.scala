package com.spotify.scio.avro

import org.apache.avro.LogicalTypes
import org.apache.avro.data.TimeConversions
import org.apache.avro.specific.{SpecificDatumReader, SpecificDatumWriter}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroDatumFactoryTest extends AnyFlatSpec with Matchers {

  "SpecificRecordDatumFactory" should "patch 1.8 model with conversions" in {
    val factory = new SpecificRecordDatumFactory(classOf[LogicalTypesTest])
    val schema = LogicalTypesTest.getClassSchema

    {
      val writer = factory(schema)
      val data = writer.asInstanceOf[SpecificDatumWriter[LogicalTypesTest]].getData
      // top-level
      data.getConversionFor(LogicalTypes.timestampMillis()) shouldBe a[TimeConversions.TimestampConversion]
      // nested-level
      data.getConversionFor(LogicalTypes.date()) shouldBe a[TimeConversions.DateConversion]
      data.getConversionFor(LogicalTypes.timeMillis()) shouldBe a[TimeConversions.TimeConversion]
    }

    {
      val reader = factory(schema, schema)
      val data = reader.asInstanceOf[SpecificDatumReader[LogicalTypesTest]].getData
      // top-level
      data.getConversionFor(LogicalTypes.timestampMillis()) shouldBe a[TimeConversions.TimestampConversion]
      // nested-level
      data.getConversionFor(LogicalTypes.date()) shouldBe a[TimeConversions.DateConversion]
      data.getConversionFor(LogicalTypes.timeMillis()) shouldBe a[TimeConversions.TimeConversion]
    }
  }

  it should "allow classes with 'conversions' field" in {
    val f = new SpecificRecordDatumFactory(classOf[NameConflict])
    val schema = LogicalTypesTest.getClassSchema
    noException shouldBe thrownBy(f(schema))
    noException shouldBe thrownBy(f(schema, schema))
  }

}
