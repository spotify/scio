/*
 * Copyright 2024 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.coders.instances

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util
import scala.jdk.CollectionConverters._
class JCollectionWrappersTest extends AnyFlatSpec with Matchers {

  "JIterableWrapper" should "extract underlying java iterator" in {
    val list = new util.ArrayList[String]()
    list.add("hey!")
    list.add("how")
    list.add("are")
    list.add("you?")
    val iterable: java.lang.Iterable[String] = list

    iterable.asScala match {
      case JavaCollectionWrappers.JIterableWrapper(underlying) =>
        underlying shouldBe iterable
      case _ =>
        fail("Could not extract underlying java iterator")
    }
  }
}
