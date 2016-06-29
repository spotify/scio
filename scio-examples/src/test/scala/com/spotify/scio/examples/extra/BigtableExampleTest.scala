/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.examples.extra

import com.spotify.scio.{bigtable => bt}
import com.spotify.scio.testing._
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.{CellUtil, HConstants, KeyValue}

import scala.collection.JavaConverters._

object BigtableExampleTest {
  // convert Put to basic comparable types
  def comparablePut(p: Put): (String, Map[String, Seq[(String, String)]]) = {
    def encode(b: Array[Byte], offset: Int = 0, length: Int = Int.MaxValue) =
      new String(b, offset, scala.math.min(length, b.length))

    val row = encode(p.getRow)
    val map = p.getFamilyCellMap.asScala.map { case (k, v) =>
      val family = encode(k)
      val cells = v.asScala.map { c =>
        val qualifier = encode(c.getQualifierArray, c.getQualifierOffset, c.getQualifierLength)
        val value = encode(c.getValueArray, c.getValueOffset, c.getValueLength)
        (qualifier, value)
      }
      (family, cells)
    }.toMap
    (row, map)
  }
}

class BigtableExampleTest extends PipelineSpec {

  import BigtableExampleTest._

  val bigtableOptions = Seq(
    "--bigtableProjectId=my-project",
    "--bigtableInstanceId=my-instance",
    "--bigtableTableId=my-table")

  val textIn = Seq("a b c d e", "a b a b")
  val wordCount = Seq(("a", 3L), ("b", 3L), ("c", 1L), ("d", 1L), ("e", 1L))
  val expectedPuts = wordCount.map(kv => BigtableExample.put(kv._1, kv._2))

  "BigtableV1WriteExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.BigtableWriteExample.type]
      .args(bigtableOptions :+ "--input=in.txt": _*)
      .input(TextIO("in.txt"), textIn)
      .output(bt.BigtableOutput[Put]("my-project", "my-instance", "my-table")) {
        _.map(comparablePut) should containInAnyOrder (expectedPuts.map(comparablePut))
      }
      .run()
  }

  def result(key: String, value: Long): Result = {
    val cell = CellUtil.createCell(
      key.getBytes, BigtableExample.FAMILY, BigtableExample.QUALIFIER,
      HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode, value.toString.getBytes)
    Result.create(Array(cell))
  }

  val resultIn = wordCount.map(kv => result(kv._1, kv._2))
  val expectedText = wordCount.map(kv => kv._1 + ": " + kv._2)

  "BigtableReadExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.BigtableReadExample.type]
      .args(bigtableOptions :+ "--output=out.txt": _*)
      .input(bt.BigtableInput("my-project", "my-instance", "my-table"), resultIn)
      .output(TextIO("out.txt"))(_ should containInAnyOrder (expectedText))
      .run()
  }

  val kingLear = Seq("a b", "a b c")
  val othello = Seq("b c ", "b c d")
  val expectedMultiple = Seq(
    "kinglear" ->
      Iterable("a" -> 2L, "b" -> 2L, "c" -> 1L).map(kv => BigtableExample.put(kv._1, kv._2)),
    "othello" ->
      Iterable("b" -> 2L, "c" -> 2L, "d" -> 1L).map(kv => BigtableExample.put(kv._1, kv._2))
  )

  "BigtableMultipleWriteExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.BigtableMultipleWriteExample.type]
      .args(bigtableOptions ++ Seq("--kinglear=k.txt", "--othello=o.txt"): _*)
      .input(TextIO("k.txt"), kingLear)
      .input(TextIO("o.txt"), othello)
      .output(bt.MultipleBigtableOutput[Put]("my-project", "my-instance")) {
        _.mapValues(_.map(comparablePut).toSet) should containInAnyOrder (
          expectedMultiple.map(kv => (kv._1, kv._2.map(comparablePut).toSet)))
      }
      .run()
  }

}
