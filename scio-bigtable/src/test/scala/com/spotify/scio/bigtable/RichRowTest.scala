/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.bigtable

import com.google.bigtable.v2.{Cell, Column, Family, Row}
import com.google.protobuf.ByteString
import org.scalatest._

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

class RichRowTest extends FlatSpec with Matchers {

  def bs(s: String): ByteString = ByteString.copyFromUtf8(s)

  val FAMILY_NAME = "family"

  val dataMap = Seq(
    "a" -> Seq(10 -> "x", 9 -> "y", 8 -> "z"),
    "b" -> Seq(7 -> "u", 6 -> "v", 5 -> "w"),
    "c" -> Seq(4 -> "r", 3 -> "s", 2 -> "t"))
    .map { case (q, cs) =>
      val kvs = cs.map(kv => (kv._1.toLong, bs(kv._2)))
      (bs(q), ListMap(kvs: _*))
    }
    .toMap

  val columns = dataMap.map { case (q, cs) =>
    val cells = cs.map { case (t, v) =>
      Cell.newBuilder().setTimestampMicros(t).setValue(v).build()
    }
    Column.newBuilder()
      .setQualifier(q)
      .addAllCells(cells.asJava)
      .build()
  }

  val row = Row.newBuilder()
    .addFamilies(Family.newBuilder()
      .setName(FAMILY_NAME)
      .addAllColumns(columns.asJava))
    .build()

  "RichRow" should "support getColumnCells" in {
    for ((q, cs) <- dataMap) {
      val cells = cs.map { case (t, v) =>
        Cell.newBuilder().setTimestampMicros(t).setValue(v).build()
      }
      row.getColumnCells(FAMILY_NAME, q) shouldBe cells
    }
  }

  it should "support getColumnLatestCell" in {
    for ((q, cs) <- dataMap) {
      val cells = cs.map { case (t, v) =>
        Cell.newBuilder().setTimestampMicros(t).setValue(v).build()
      }
      row.getColumnLatestCell(FAMILY_NAME, q) shouldBe cells.headOption
    }
  }

  it should "support getFamilyMap" in {
    val familyMap = dataMap.map { case (q, cs) => (q, cs.head._2)}
    row.getFamilyMap(FAMILY_NAME) shouldBe familyMap
  }

  it should "support getMap" in {
    row.getMap shouldBe Map(FAMILY_NAME -> dataMap)
  }

  it should "support getNoVersionMap" in {
    val noVerMap = dataMap.map { case (q, cs) => (q, cs.head._2)}
    row.getNoVersionMap shouldBe Map(FAMILY_NAME -> noVerMap)
  }

  it should "support getValue" in {
    for ((q, cs) <- dataMap) {
      row.getValue(FAMILY_NAME, q) shouldBe Some(cs.head._2)
    }
  }

}
