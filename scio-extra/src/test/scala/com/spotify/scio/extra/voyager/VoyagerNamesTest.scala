/*
 * Copyright 2026 Spotify AB.
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

package com.spotify.scio.extra.voyager

import com.spotify.voyager.jni.utils.TinyJson
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._

class VoyagerNamesTest extends AnyFlatSpec with Matchers {
  private def parse(s: String): Seq[String] =
    VoyagerReader.parseNames(s.getBytes(StandardCharsets.UTF_8)).toSeq

  private def tinyJson(names: Seq[String]): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    TinyJson.writeStringList(names.asJava, out)
    out.toByteArray
  }

  "VoyagerReader.parseNames" should "parse names written by voyager" in {
    val names = Seq("a", "", "spotify:track:123", "ünïcødé 🎵", "with space", "x,y", "[]")
    VoyagerReader.parseNames(tinyJson(names)).toSeq shouldBe names
  }

  it should "parse empty lists" in {
    parse("[]") shouldBe empty
    parse(" [ ] ") shouldBe empty
    VoyagerReader.parseNames(tinyJson(Seq.empty)).toSeq shouldBe empty
  }

  it should "tolerate whitespace" in {
    parse(" [ \"a\" ,\n\"b\"\t]\n") shouldBe Seq("a", "b")
  }

  it should "agree with TinyJson" in {
    val bytes = tinyJson((0 until 10000).map(i => s"name-$i"))
    VoyagerReader.parseNames(bytes).toSeq shouldBe
      TinyJson.readStringList(new ByteArrayInputStream(bytes)).asScala.toSeq
  }

  it should "reject malformed input" in {
    for (
      bad <- Seq(
        "",
        "\"a\"",
        "[\"a\"",
        "[\"a\" \"b\"]",
        "[\"a\",]",
        "[,\"a\"]",
        "[\"a",
        "{}",
        "[1, \"a\"]",
        "[true]",
        "[null]",
        "[\"a\", [\"b\"]]",
        "[\"a\"] x",
        "[\"a\"][\"b\"]"
      )
    )
      withClue(bad)(an[IllegalArgumentException] should be thrownBy parse(bad))
  }

}
