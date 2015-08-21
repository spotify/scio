package com.spotify.cloud.dataflow.coders

import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.values.KV
import com.spotify.scio.avro.TestRecord
import com.spotify.cloud.dataflow.coders.CoderTestUtils._
import com.spotify.cloud.dataflow.testing.PipelineTest
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.ClassTag

class KryoAtomicCoderTest extends FlatSpec with Matchers with PipelineTest {

  type CoderFactory = () => Coder[Any]
  val cf = () => new KryoAtomicCoder

  class RoundTripMatcher[T: ClassTag](value: T) extends Matcher[CoderFactory] {
    override def apply(left: CoderFactory): MatchResult = {
      MatchResult(
        testRoundTrip(left(), left(), value),
        s"CoderRegistry did not round trip $value",
        s"CoderRegistry did round trip $value")
    }
  }

  def roundTrip[T: ClassTag](value: T) = new RoundTripMatcher[T](value)

  "KryoAtomicCoder" should "support Scala collections" in {
    cf should roundTrip (Seq(1, 2, 3))
    cf should roundTrip (List(1, 2, 3))
    cf should roundTrip (Set(1, 2, 3))
    cf should roundTrip (Map("a" -> 1, "b" -> 2, "c" -> 3))
  }

  it should "support Scala tuples" in {
    cf should roundTrip (("hello", 10))
    cf should roundTrip (("hello", 10, 10.0))
    cf should roundTrip (("hello", (10, 10.0)))
  }

  it should "support Scala case classes" in {
    cf should roundTrip (Pair("record", 10))
  }

  it should "support wrapped iterables" in {
    cf should roundTrip (iterable(1, 2, 3))
  }

  it should "support Avro GenericRecord" in {
    val r = newGenericRecord
    cf should roundTrip (r)
    cf should roundTrip (("key", r))
    cf should roundTrip (CaseClassWithGenericRecord("record", 10, r))
  }

  it should "support Avro SpecificRecord" in {
    val r = new TestRecord(1, 1L, 1F, 1.0, true, "hello")
    cf should roundTrip (r)
    cf should roundTrip (("key", r))
    cf should roundTrip (CaseClassWithSpecificRecord("record", 10, r))
  }

  it should "support KV" in {
    cf should roundTrip (KV.of("key", 1.0))
    cf should roundTrip (KV.of("key", (10, 10.0)))
    cf should roundTrip (KV.of("key", new TestRecord(1, 1L, 1F, 1.0, true, "hello")))
    cf should roundTrip (KV.of("key", newGenericRecord))
  }

}
