package com.spotify.cloud.dataflow.coders

import com.google.cloud.dataflow.sdk.coders.CoderRegistry
import com.google.cloud.dataflow.sdk.testing.TestPipeline
import com.spotify.cloud.dataflow.Implicits
import com.spotify.cloud.dataflow.avro.TestRecord
import com.spotify.cloud.dataflow.coders.CoderTestUtils._
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{Matchers, FlatSpec}

import scala.reflect.ClassTag

class RichCoderRegistryTest extends FlatSpec with Matchers with Implicits {

  val pipeline = TestPipeline.create()
  val registry = pipeline.getCoderRegistry
  registry.registerScalaCoders()

  class RoundTripMatcher[T: ClassTag](value: T) extends Matcher[CoderRegistry] {
    override def apply(left: CoderRegistry): MatchResult = {
      val coder = left.getScalaCoder[T]
      coder shouldNot be (null)
      MatchResult(
        testRoundTrip(coder, value),
        s"CoderRegistry did not round trip $value",
        s"CoderRegistry did round trip $value")

    }
  }

  def roundTrip[T: ClassTag](value: T) = new RoundTripMatcher[T](value)

  "RichCoderRegistry" should "support Scala primitives" in {
    registry should roundTrip (10)
    registry should roundTrip (10L)
    registry should roundTrip (10F)
    registry should roundTrip (10.0)
  }

  it should "support Scala tuples" in {
    registry should roundTrip (("hello", 10))
    registry should roundTrip (("hello", 10, 10.0))
    registry should roundTrip (("hello", (10, 10.0)))
  }

  it should "support Scala case classes" in {
    registry should roundTrip (Pair("record", 10))
  }

  it should "support Avro records" in {
    val r = new TestRecord(1, 1L, 1F, 1.0, true, "hello")
    registry should roundTrip (r)
    registry should roundTrip (("key", r))
    registry should roundTrip (PairWithAvro("record", 10, r))
  }

}
