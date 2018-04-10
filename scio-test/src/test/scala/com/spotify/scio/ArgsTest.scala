/*
 * Copyright 2018 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio

import org.scalatest.{FlatSpec, Matchers}

class ArgsTest extends FlatSpec with Matchers {

  "Args" should "support String" in {
    Args("--str=value".split(" "))("str") shouldBe "value"
  }

  it should "support getOrElse" in {
    Args("--key1=value1".split(" ")).getOrElse("key2", "value2") shouldBe "value2"
  }

  it should "support list" in {
    Args("--key=value1 --key=value2".split(" ")).list("key") shouldBe List("value1", "value2")
  }

  it should "support optional" in {
    val args = Args("--key1=value1".split(" "))
    args.optional("key1") shouldBe Some("value1")
    args.optional("key2") shouldBe None
  }

  it should "support required" in {
    Args("--key=value".split(" ")).required("key") shouldBe "value"
  }

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail required with missing value" in {
    the [IllegalArgumentException] thrownBy {
      Args(Array.empty).required("key")
    } should have message "Missing value for property 'key'"
  }

  it should "fail required with multiple values" in {
    the [IllegalArgumentException] thrownBy {
      Args("--key=value1 --key=value2".split(" ")).required("key")
    } should have message "Multiple values for property 'key'"
  }
  // scalastyle:on no.whitespace.before.left.bracket

  it should "support int" in {
    val args = Args("--key1=10".split(" "))
    args.int("key1") shouldBe 10
    args.int("key2", 20) shouldBe 20
  }

  it should "support long" in {
    val args = Args("--key1=10".split(" "))
    args.long("key1") shouldBe 10L
    args.long("key2", 20L) shouldBe 20L
  }

  it should "support float" in {
    val args = Args("--key1=1.5".split(" "))
    args.float("key1") shouldBe 1.5F
    args.float("key2", 2.5F) shouldBe 2.5F
  }

  it should "support double" in {
    val args = Args("--key1=1.5".split(" "))
    args.double("key1") shouldBe 1.5
    args.double("key2", 2.5) shouldBe 2.5
  }

  it should "support boolean" in {
    val args = Args("--key1=true --key2=false --key3".split(" "))
    args.boolean("key1") shouldBe true
    args.boolean("key2") shouldBe false
    args.boolean("key3") shouldBe true
    args.boolean("key4", true) shouldBe true
    args.boolean("key5", false) shouldBe false
  }

  it should "support quotes" in {
    def list(s: String): List[String] = Args(Array(s"--list=$s")).list("list")
    list("a,b,c") shouldBe List("a", "b", "c")
    list(",a,b") shouldBe List("", "a", "b")
    list("a,,b") shouldBe List("a", "", "b")
    list("a,b,") shouldBe List("a", "b", "")
    list("\"a1,a2\",b,c") shouldBe List("\"a1,a2\"", "b", "c")
    list("a,\"b1,b2\",c") shouldBe List("a", "\"b1,b2\"", "c")
    list("a,b,\"c1,c2\"") shouldBe List("a", "b", "\"c1,c2\"")
    list("a,\"b1, b2\",c") shouldBe List("a", "\"b1, b2\"", "c")
    list("a,b0 \"b1, b2\" b3,c") shouldBe List("a", "b0 \"b1, b2\" b3", "c")
  }

  it should "support toString" in {
    val args = Args(Array("--key1=value1", "--key2=value2", "--key2=value3", "--key3"))
    args.toString shouldBe "Args(--key1=value1, --key2=[value2, value3], --key3=true)"
  }

}
