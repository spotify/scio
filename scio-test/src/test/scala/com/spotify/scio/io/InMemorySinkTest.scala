package com.spotify.scio.io

import com.spotify.scio.ScioContext
import org.scalatest.{FlatSpec, Matchers}

class InMemorySinkTest extends FlatSpec with Matchers {

  "InMemoryTap" should "return containing items as iterable" in {
    val sc = ScioContext.forTest()
    val items = sc.parallelize(List("String1", "String2"))
    val tap = TapOf[String].saveForTest(items)
    sc.close().waitUntilDone()
    tap.value.toList should contain allOf ("String1", "String2")
  }

  it should "return empty iterable when SCollection is empty" in {
    val sc = ScioContext.forTest()
    val items = sc.parallelize[String](List())
    val tap = TapOf[String].saveForTest(items)
    sc.close().waitUntilDone()
    tap.value shouldBe empty
  }

}
