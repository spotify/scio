package com.spotify.scio.testing.util
import org.scalactic.Prettifier
import org.scalatest.{FlatSpec, Matchers}

// A class which Scio's Schema derivation will use fallback
class NoSchemaAvailable() extends Serializable {
  override def toString: String = "ClassWithNoSchemaAvailable"
}

class SCollectionPrettifierTest extends FlatSpec with Matchers {
  case class NestedRecord(p: String)
  case class Something(a: Int, b: NestedRecord)

  behavior of "SCollectionPrettifier"

  it should "prettify an SCollection with Schema" in {
    val prettyString =
      implicitly[TypedPrettifier[Something]].apply(Seq(Something(1, NestedRecord("one"))))

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

  it should "use default Prettifier if Schema is not available" in {
    val prettyString =
      implicitly[TypedPrettifier[NoSchemaAvailable]].apply(Seq(new NoSchemaAvailable()))

    val expected = "List(ClassWithNoSchemaAvailable)"

    prettyString should be(expected)
  }

  it should "use fallback to implicit prettifier available in scope" in {
    implicit val testPrettifier: Prettifier = new Prettifier {
      override def apply(o: Any): String = "user - defined"
    }

    val prettyString =
      implicitly[TypedPrettifier[Something]].apply(Seq(Something(1, NestedRecord("one"))))

    val expected =
      """
        |┌──────────────────────────────┬──────────────────────────────┐
        |│a                             │b                             │
        |├──────────────────────────────┼──────────────────────────────┤
        |│user - defined                │user - defined                │
        |└──────────────────────────────┴──────────────────────────────┘
        |""".stripMargin

    prettyString should be(expected)
  }
}
