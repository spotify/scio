/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.bigquery

import com.spotify.scio.bigquery.BigQueryTypedTable.Format
import com.spotify.scio.{ContextAndArgs, ScioContext}
import com.spotify.scio.testing._
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.Pipeline.PipelineVisitor
import org.apache.beam.sdk.io.gcp.{bigquery => beam}
import org.apache.beam.sdk.io.Read
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition
import org.apache.beam.sdk.runners.TransformHierarchy
import org.apache.beam.sdk.transforms.display.DisplayData
import org.apache.beam.sdk.values.PValue

import scala.collection.mutable
import scala.jdk.CollectionConverters._

object BigQueryIOTest {
  @BigQueryType.toTable
  case class BQRecord(i: Int, s: String, r: List[String])

  // BQ Write transform display id data for tableDescription
  private val TableDescriptionId = DisplayData.Identifier.of(
    DisplayData.Path.root(),
    classOf[beam.BigQueryIO.Write[_]],
    "tableDescription"
  )

  /**
   * Return `Read` Transforms that do not have another transform using it as an input.
   *
   * To do this, we visit all PTransforms, and find the inputs at each stage, and mark those inputs
   * as consumed by putting them in `consumedOutputs`. We also check if each transform is a `Read`
   * and if so we extract them as well.
   *
   * This is copied from Beam's test for UnconsumedReads.
   */
  def unconsumedReads(context: ScioContext): Set[PValue] = {
    val consumedOutputs = mutable.HashSet[PValue]()
    val allReads = mutable.HashSet[PValue]()

    context.pipeline.traverseTopologically(
      new PipelineVisitor.Defaults {
        override def visitPrimitiveTransform(node: TransformHierarchy#Node): Unit =
          consumedOutputs ++= node.getInputs.values().asScala

        override def visitValue(
          value: PValue,
          producer: TransformHierarchy#Node
        ): Unit =
          producer.getTransform match {
            case _: Read.Bounded[_] | _: Read.Unbounded[_] =>
              allReads += value
            case _ =>
          }
      }
    )

    allReads.diff(consumedOutputs).toSet
  }
}

final class BigQueryIOTest extends ScioIOSpec {
  import BigQueryIOTest._

  "BigQueryIO" should "apply config override" in {
    val name = "saveAsBigQueryTable"
    val desc = "table-description"
    val sc = ScioContext()
    val io = BigQueryTypedTable[GenericRecord](
      table = Table.Spec("project:dataset.out_table"),
      format = Format.GenericRecord
    )
    val params = BigQueryTypedTable.WriteParam[GenericRecord](
      createDisposition = CreateDisposition.CREATE_NEVER,
      configOverride = _.withTableDescription(desc)
    )
    sc.empty[GenericRecord]()
      .withName(name)
      .write(io)(params)

    val finder = new TransformFinder(new EqualNamePTransformMatcher(name))
    sc.pipeline.traverseTopologically(finder)
    val transform = finder.result().head
    val displayData = DisplayData.from(transform).asMap().asScala
    displayData should contain key TableDescriptionId
    displayData(TableDescriptionId).getValue shouldBe desc
  }

  it should "work with TableRow" in {
    val xs = (1 to 100).map(x => TableRow("x" -> x.toString))
    testJobTest(xs, in = "project:dataset.in_table", out = "project:dataset.out_table")(
      BigQueryIO(_)
    )((sc, s) => sc.bigQueryTable(Table.Spec(s)))((coll, s) =>
      coll.saveAsBigQueryTable(Table.Spec(s))
    )
  }

  /**
   * The `BigQueryIO`'s write, runs Beam's BQ IO which creates a `Read` Transform to return the
   * insert errors.
   *
   * The `saveAsBigQuery` or `saveAsTypedBigQuery` in Scio is designed to return a `ClosedTap` and
   * by default drops insert errors.
   *
   * The following tests make sure that the dropped insert errors do not appear as an unconsumed
   * read outside the transform writing to Big Query.
   */
  it should "not have unconsumed errors with saveAsBigQuery" in {
    val xs = (1 to 100).map(x => TableRow("x" -> x.toString))

    val context = ScioContext()
    context
      .parallelize(xs)
      .saveAsBigQueryTable(Table.Spec("project:dataset.dummy"), createDisposition = CREATE_NEVER)
    // We want to validate on the job graph, and we need not actually execute the pipeline.

    unconsumedReads(context) shouldBe empty
  }

  it should "not have unconsumed errors with saveAsTypedBigQuery" in {
    val xs = (1 to 100).map(x => BQRecord(x, x.toString, (1 to x).map(_.toString).toList))

    val context = ScioContext()
    context
      .parallelize(xs)
      .saveAsTypedBigQueryTable(
        Table.Spec("project:dataset.dummy"),
        createDisposition = CREATE_NEVER
      )
    // We want to validate on the job graph, and we need not actually execute the pipeline.

    unconsumedReads(context) shouldBe empty
  }

  it should "read the same input table with different predicate and projections using bigQueryStorage" in {

    JobTest[JobWithDuplicateInput.type]
      .args("--input=table.in")
      .input(
        BigQueryIO[TableRow]("table.in", List("a"), Some("a > 0")),
        (1 to 3).map(x => TableRow("x" -> x.toString))
      )
      .input(
        BigQueryIO[TableRow]("table.in", List("b"), Some("b > 0")),
        (1 to 3).map(x => TableRow("x" -> x.toString))
      )
      .run()

  }

  it should "read the same input table with different predicate and projections using typedBigQueryStorage" in {

    JobTest[TypedJobWithDuplicateInput.type]
      .args("--input=table.in")
      .input(
        BigQueryIO[BQRecord]("table.in", List("a"), Some("a > 0")),
        (1 to 3).map(x => BQRecord(x, x.toString, (1 to x).map(_.toString).toList))
      )
      .input(
        BigQueryIO[BQRecord]("table.in", List("b"), Some("b > 0")),
        (1 to 3).map(x => BQRecord(x, x.toString, (1 to x).map(_.toString).toList))
      )
      .run()

  }

  "TableRowJsonIO" should "work" in {
    val xs = (1 to 100).map(x => TableRow("x" -> x.toString))
    testTap(xs)(_.saveAsTableRowJsonFile(_))(".json")
    testJobTest(xs)(TableRowJsonIO(_))(_.tableRowJsonFile(_))(_.saveAsTableRowJsonFile(_))
  }

}

object JobWithDuplicateInput {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.bigQueryStorage(Table.Spec(args("input")), List("a"), "a > 0")
    sc.bigQueryStorage(Table.Spec(args("input")), List("b"), "b > 0")
    sc.run()
    ()
  }
}

object TypedJobWithDuplicateInput {
  import BigQueryIOTest._

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.typedBigQueryStorage[BQRecord](Table.Spec(args("input")), List("a"), "a > 0")
    sc.typedBigQueryStorage[BQRecord](Table.Spec(args("input")), List("b"), "b > 0")
    sc.run()
    ()
  }
}
