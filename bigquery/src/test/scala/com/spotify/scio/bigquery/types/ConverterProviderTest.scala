package com.spotify.scio.bigquery.types

import com.google.common.collect.Maps
import com.spotify.scio.bigquery.TableRow
import org.joda.time.Instant
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.JavaConverters._

class ConverterProviderTest extends FlatSpec with Matchers {

  import Schemas._

  def jl[T](x: T*): java.util.List[T] = new java.util.ArrayList[T](x.asJava)

  def linkedHashMap(pairs: (String, Any)*) =
    Maps.newLinkedHashMap[String, AnyRef](Map(pairs: _*).asJava.asInstanceOf[java.util.Map[String, AnyRef]])

  val NOW = Instant.now()

  val t1 = TableRow("f1" -> 1, "f2" -> 10L, "f3" -> 1.2f, "f4" -> 1.23, "f5" -> true, "f6" -> "hello", "f7" -> NOW)
  val m1 = linkedHashMap("f1" -> 1, "f2" -> 10L, "f3" -> 1.2f, "f4" -> 1.23, "f5" -> true, "f6" -> "hello", "f7" -> NOW)
  val p1 = P1(1, 10L, 1.2f, 1.23, true, "hello", NOW)

  val t2a = TableRow("f1" -> 1, "f2" -> 10L, "f3" -> 1.2f, "f4" -> 1.23, "f5" -> true, "f6" -> "hello", "f7" -> NOW)
  val m2a = linkedHashMap("f1" -> 1, "f2" -> 10L, "f3" -> 1.2f, "f4" -> 1.23, "f5" -> true, "f6" -> "hello", "f7" -> NOW)
  val t2b = TableRow("f1" -> null, "f2" -> null, "f3" -> null, "f4" -> null, "f5" -> null, "f6" -> null, "f7" -> null)
  val p2a = P2(Some(1), Some(10L), Some(1.2f), Some(1.23), Some(true), Some("hello"), Some(NOW))
  val p2b = P2(None, None, None, None, None, None, None)

  val t3a = TableRow(
    "f1" -> jl(1), "f2" -> jl(10L), "f3" -> jl(1.2f), "f4" -> jl(1.23),
    "f5" -> jl(true), "f6" -> jl("hello"), "f7" -> jl(NOW))
  val m3a = linkedHashMap(
    "f1" -> jl(1), "f2" -> jl(10L), "f3" -> jl(1.2f), "f4" -> jl(1.23),
    "f5" -> jl(true), "f6" -> jl("hello"), "f7" -> jl(NOW))
  val t3b = TableRow("f1" -> jl(), "f2" -> jl(), "f3" -> jl(), "f4" -> jl(), "f5" -> jl(), "f6" -> jl(), "f7" -> jl())
  val p3a = P3(List(1), List(10L), List(1.2f), List(1.23), List(true), List("hello"), List(NOW))
  val p3b = P3(Nil, Nil, Nil, Nil, Nil, Nil, Nil)

  "BigQueryEntity.fromTableRow" should "support required primitive types" in {
    BigQueryType.fromTableRow[P1](t1) should equal (p1)
  }

  it should "support nullable primitive types" in {
    BigQueryType.fromTableRow[P2](t2a) should equal (p2a)
    BigQueryType.fromTableRow[P2](t2b) should equal (p2b)
  }

  it should "support repeated primitive types" in {
    BigQueryType.fromTableRow[P3](t3a) should equal (p3a)
    BigQueryType.fromTableRow[P3](t3b) should equal (p3b)
  }

  it should "support required records" in {
    val r1 = TableRow("f1" -> t1, "f2" -> t2a, "f3" -> t3a)
    BigQueryType.fromTableRow[R1](r1) should equal (R1(p1, p2a, p3a))
  }

  it should "support nullable records" in {
    val r2a = TableRow("f1" -> t1, "f2" -> t2a, "f3" -> t3a)
    val r2b = TableRow("f1" -> null, "f2" -> null, "f3" -> null)
    BigQueryType.fromTableRow[R2](r2a) should equal (R2(Some(p1), Some(p2a), Some(p3a)))
    BigQueryType.fromTableRow[R2](r2b) should equal (R2(None, None, None))
  }

  it should "support repeated records" in {
    val r3a = TableRow("f1" -> jl(t1), "f2" -> jl(t2a), "f3" -> jl(t3a))
    val r3b = TableRow("f1" -> jl(), "f2" -> jl(), "f3" -> jl())
    BigQueryType.fromTableRow[R3](r3a) should equal (R3(List(p1), List(p2a), List(p3a)))
    BigQueryType.fromTableRow[R3](r3b) should equal (R3(Nil, Nil, Nil))
  }

  it should "support LinkedHashMap records" in {
    val r1 = TableRow("f1" -> m1, "f2" -> m2a, "f3" -> m3a)
    BigQueryType.fromTableRow[R1](r1) should equal (R1(p1, p2a, p3a))
  }

  "BigQueryEntity.toTableRow" should "support required primitive types" in {
    BigQueryType.toTableRow[P1](p1) should equal (t1)
  }

  it should "support nullable primitive types" in {
    BigQueryType.toTableRow[P2](p2a) should equal (t2a)
    BigQueryType.toTableRow[P2](p2b) should equal (t2b)
  }

  it should "support repeated primitive types" in {
    BigQueryType.toTableRow[P3](p3a) should equal (t3a)
    BigQueryType.toTableRow[P3](p3b) should equal (t3b)
  }

  it should "support required records" in {
    val r1 = TableRow("f1" -> t1, "f2" -> t2a, "f3" -> t3a)
    BigQueryType.toTableRow[R1](R1(p1, p2a, p3a)) should equal (r1)
  }

  it should "support nullable records" in {
    val r2a = TableRow("f1" -> t1, "f2" -> t2a, "f3" -> t3a)
    val r2b = TableRow("f1" -> null, "f2" -> null, "f3" -> null)
    BigQueryType.toTableRow[R2](R2(Some(p1), Some(p2a), Some(p3a))) should equal (r2a)
    BigQueryType.toTableRow[R2](R2(None, None, None)) should equal (r2b)
  }

  it should "support repeated records" in {
    val r3a = TableRow("f1" -> jl(t1), "f2" -> jl(t2a), "f3" -> jl(t3a))
    val r3b = TableRow("f1" -> jl(), "f2" -> jl(), "f3" -> jl())
    BigQueryType.toTableRow[R3](R3(List(p1), List(p2a), List(p3a))) should equal (r3a)
    BigQueryType.toTableRow[R3](R3(Nil, Nil, Nil)) should equal (r3b)
  }

}
