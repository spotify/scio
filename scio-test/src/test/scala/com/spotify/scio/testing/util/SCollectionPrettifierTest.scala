package com.spotify.scio.testing.util
import com.spotify.scio.avro.TestRecord
import org.scalactic.Prettifier
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

// A class which Scio's Schema derivation will use fallback
class NoSchemaAvailable() extends Serializable {
  override def toString: String = "ClassWithNoSchemaAvailable"
}

class SCollectionPrettifierTest extends AnyFlatSpec with Matchers {

  private val avroRecods: Iterable[TestRecord] = (1 to 5).map(i =>
    TestRecord
      .newBuilder()
      .setIntField(i)
      .setStringField(i.toString)
      .setBooleanField(false)
      .setDoubleField(i / 2.0)
      .build()
  )

  case class NestedRecord(p: String)
  case class Something(a: Int, b: NestedRecord)

  behavior of "SCollectionPrettifier"

  it should "prettify AvroRecords" in {
    val prettyString =
      TypedPrettifier[TestRecord](avroRecods)

    val expected =
      """
        |┌──────────────────────────────┬──────────────────────────────┬──────────────────────────────┬──────────────────────────────┬──────────────────────────────┬──────────────────────────────┬──────────────────────────────┐
        |│int_field                     │long_field                    │float_field                   │double_field                  │boolean_field                 │string_field                  │array_field                   │
        |├──────────────────────────────┼──────────────────────────────┼──────────────────────────────┼──────────────────────────────┼──────────────────────────────┼──────────────────────────────┼──────────────────────────────┤
        |│1                             │null                          │null                          │0.5                           │false                         │"1"                           │[]                            │
        |│2                             │null                          │null                          │1.0                           │false                         │"2"                           │[]                            │
        |│3                             │null                          │null                          │1.5                           │false                         │"3"                           │[]                            │
        |│4                             │null                          │null                          │2.0                           │false                         │"4"                           │[]                            │
        |│5                             │null                          │null                          │2.5                           │false                         │"5"                           │[]                            │
        |└──────────────────────────────┴──────────────────────────────┴──────────────────────────────┴──────────────────────────────┴──────────────────────────────┴──────────────────────────────┴──────────────────────────────┘
        |""".stripMargin
    prettyString should be(expected)
  }

  ignore should "prettify an SCollection with Schema" in {
    val prettyString =
      TypedPrettifier[Something](Seq(Something(1, NestedRecord("one"))))

    val expected =
      """
      |┌──────────────────────────────┬──────────────────────────────┐
      |│a                             │b                             │
      |├──────────────────────────────┼──────────────────────────────┤
      |│1                             │Row:[one]                     │
      |└──────────────────────────────┴──────────────────────────────┘
      |""".stripMargin

    prettyString should be(expected)
  }

  it should "use default Prettifier for non-avro records" in {
    val prettyString =
      TypedPrettifier[Something](Seq(Something(1, NestedRecord("one"))))

    val expected = "List(Something(1,NestedRecord(one)))"
    prettyString should be(expected)
  }

  it should "use default default scalactic prettifier if Schema is not available" in {
    val prettyString =
      TypedPrettifier[NoSchemaAvailable](Seq(new NoSchemaAvailable()))

    val expected = "List(ClassWithNoSchemaAvailable)"

    prettyString should be(expected)
  }

  it should "use fallback to implicit prettifier available in scope" in {
    implicit val testPrettifier: Prettifier = new Prettifier {
      override def apply(o: Any): String = "user - defined"
    }

    val prettyString =
      TypedPrettifier[Something](Seq(Something(1, NestedRecord("one"))))

    val expected = "user - defined"

    prettyString should be(expected)
  }
}
