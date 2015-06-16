package com.spotify.cloud.dataflow.coders

import com.google.cloud.dataflow.sdk.coders.Coder
import com.google.cloud.dataflow.sdk.values.KV
import com.spotify.cloud.dataflow.coders.CoderTestUtils._
import com.spotify.cloud.dataflow.avro.TestRecord
import com.spotify.cloud.dataflow.testing.PipelineTest
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{Matchers, FlatSpec}

import scala.reflect.ClassTag

class KryoAtomicCoderTest extends FlatSpec with Matchers with PipelineTest {

  val c = new KryoAtomicCoder()

  class RoundTripMatcher[T: ClassTag](value: T) extends Matcher[Coder[Any]] {
    override def apply(left: Coder[Any]): MatchResult = {
      MatchResult(
        testRoundTrip(left, value),
        s"CoderRegistry did not round trip $value",
        s"CoderRegistry did round trip $value")

    }
  }

  def roundTrip[T: ClassTag](value: T) = new RoundTripMatcher[T](value)

  "KryoAtomicCoder" should "support Scala collections" in {
    c should roundTrip (Seq(1, 2, 3))
    c should roundTrip (List(1, 2, 3))
    c should roundTrip (Set(1, 2, 3))
    c should roundTrip (Map("a" -> 1, "b" -> 2, "c" -> 3))
  }

  it should "support Scala tuples" in {
    c should roundTrip (("hello", 10))
    c should roundTrip (("hello", 10, 10.0))
    c should roundTrip (("hello", (10, 10.0)))
  }

  it should "support Scala case classes" in {
    c should roundTrip (Pair("record", 10))
  }

  it should "support wrapped iterables" in {
    c should roundTrip (iterable(1, 2, 3))
  }

  it should "support Avro GenericRecord" in {
    val r = newGenericRecord
    c should roundTrip (r)
    c should roundTrip (("key", r))
    c should roundTrip (CaseClassWithGenericRecord("record", 10, r))
  }

  it should "support Avro SpecificRecord" in {
    val r = new TestRecord(1, 1L, 1F, 1.0, true, "hello")
    c should roundTrip (r)
    c should roundTrip (("key", r))
    c should roundTrip (CaseClassWithSpecificRecord("record", 10, r))
  }

  it should "support KV" in {
    c should roundTrip (KV.of("key", 1.0))
    c should roundTrip (KV.of("key", (10, 10.0)))
    c should roundTrip (KV.of("key", new TestRecord(1, 1L, 1F, 1.0, true, "hello")))
    c should roundTrip (KV.of("key", newGenericRecord))
  }

}
