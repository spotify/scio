/*
 * Copyright 2019 Spotify AB.
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

import cats.Eq
import com.google.cloud.bigtable.hbase.adapters.read.RowCell
import com.spotify.scio.testing._
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.client.{Mutation, Put, Result}

import java.nio.charset.StandardCharsets
import java.time.Instant
import scala.jdk.CollectionConverters._

class BigtableIOTest extends ScioIOSpec {
  val projectId = "project"
  val instanceId = "instance"

  val family: Array[Byte] = "count".getBytes(StandardCharsets.UTF_8)
  val qualifier: Array[Byte] = "int".getBytes(StandardCharsets.UTF_8)

  "BigtableIO" should "work with input" in {
    val ts = Instant.now().toEpochMilli
    implicit val eqResult: Eq[Result] = Eq.by(_.toString)
    val xs = (1 to 10).map { x =>
      val cell = new RowCell(
        x.toString.getBytes(StandardCharsets.UTF_8),
        family,
        qualifier,
        ts,
        BigInt(x).toByteArray
      )
      Result.create(List[Cell](cell).asJava)
    }

    testJobTestInput(xs)(BigtableIO(projectId, instanceId, _))(_.bigtable(projectId, instanceId, _))
  }

  it should "work with output" in {
    implicit val eqResult: Eq[Mutation] = Eq.by(_.toString)
    val xs: Seq[Mutation] = (1 to 10).map { x =>
      new Put(x.toString.getBytes(StandardCharsets.UTF_8))
        .addColumn(family, qualifier, BigInt(x).toByteArray)
    }

    testJobTestOutput(xs)(BigtableIO(projectId, instanceId, _))(
      _.saveAsBigtable(projectId, instanceId, _)
    )
  }
}
