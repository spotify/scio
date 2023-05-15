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

package com.spotify.scio.values

import com.spotify.scio.ScioContext
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.MultiJoin
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.runners.TransformHierarchy
import org.apache.beam.sdk.values.PCollection
import org.scalatest.Assertion

import scala.collection.mutable

object SimpleJob {
  import com.spotify.scio._
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val output = args("output")
    sc.parallelize(1 to 5).saveAsTextFile(output)
    sc.run()
    ()
  }
}

trait NamedTransformSpec extends PipelineSpec {
  def assertTransformNameStartsWith(p: PCollectionWrapper[_], tfName: String): Assertion = {
    val visitor = new AssertTransformNameVisitor(p.internal, tfName)
    p.context.pipeline.traverseTopologically(visitor)
    visitor.nodeFullName should startWith regex tfName
  }

  def assertGraphContainsStep(p: PCollectionWrapper[_], tfName: String): Assertion = {
    val visitor = new NameAccumulatingVisitor()
    p.context.pipeline.traverseTopologically(visitor)
    withClue(s"All nodes: ${visitor.nodes.sorted.mkString("", "\n", "\n")}") {
      visitor.nodes.flatMap(_.split('/')).toSet should contain(tfName)
    }
  }

  def assertGraphContainsStepRegex(p: PCollectionWrapper[_], tfNameRegex: String): Assertion = {
    val visitor = new NameAccumulatingVisitor()
    p.context.pipeline.traverseTopologically(visitor)
    val allNodes = visitor.nodes.sorted.mkString("", "\n", "\n")
    withClue(s"$tfNameRegex did not match a step in any of the following nodes: $allNodes") {
      visitor.nodes.flatMap(_.split('/')).toSet.filter(_.matches(tfNameRegex)) should not be empty
    }
  }

  class AssertTransformNameVisitor(pcoll: PCollection[_], tfName: String)
      extends Pipeline.PipelineVisitor.Defaults {
    val prefix: List[String] = tfName.split("[(/]").toList
    var nodeFullName = "<unknown>"

    override def visitPrimitiveTransform(node: TransformHierarchy#Node): Unit =
      if (node.getOutputs.containsValue(pcoll)) nodeFullName = node.getFullName
  }

  class NameAccumulatingVisitor extends Pipeline.PipelineVisitor.Defaults {
    var nodes: mutable.ListBuffer[String] = mutable.ListBuffer.empty[String]
    override def visitPrimitiveTransform(node: TransformHierarchy#Node): Unit =
      nodes.append(node.getFullName)
  }
}

class NamedTransformTest extends NamedTransformSpec {
  "ScioContext" should "support custom transform name" in {
    val sc = ScioContext()
    val p = sc.withName("ReadInput").parallelize(Seq("a", "b", "c"))
    assertTransformNameStartsWith(p, "ReadInput/Read")
  }

  "SCollection" should "support custom transform name" in {
    val sc = ScioContext()
    val p = sc
      .parallelize(Seq(1, 2, 3, 4, 5))
      .map(_ * 3)
      .withName("OnlyEven")
      .filter(_ % 2 == 0)
    assertTransformNameStartsWith(p, "OnlyEven")
  }

  "DoubleSCollectionFunctions" should "support custom transform name" in {
    val sc = ScioContext()
    val p = sc
      .parallelize(Seq(1.0, 2.0, 3.0, 4.0, 5.0))
      .withName("CalcVariance")
      .variance
    assertTransformNameStartsWith(p, "CalcVariance")
  }

  "PairSCollectionFunctions" should "support custom transform name" in {
    val sc = ScioContext()
    val p = sc
      .parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      .withName("SumPerKey")
      .sumByKey
    assertTransformNameStartsWith(p, "SumPerKey/KvToTuple")
  }

  "SCollectionWithFanout" should "support custom transform name" in {
    val sc = ScioContext()
    val p = sc
      .parallelize(Seq(1, 2, 3))
      .withFanout(10)
      .withName("Sum")
      .sum
    assertTransformNameStartsWith(p, "Sum/Values/Values")
  }

  "SCollectionWithHotKeyFanout" should "support custom transform name" in {
    val sc = ScioContext()
    val p = sc
      .parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      .withHotKeyFanout(10)
      .withName("Sum")
      .sumByKey
    assertTransformNameStartsWith(p, "Sum/KvToTuple")
  }

  "SCollectionWithSideInput" should "support custom transform name" in {
    val sc = ScioContext()
    val p1 = sc.parallelize(Seq("a", "b", "c"))
    val p2 = sc.parallelize(Seq(1, 2, 3)).asListSideInput
    val s = p1
      .withSideInputs(p2)
      .withName("GetX")
      .filter((x, _) => x == "a")
    assertTransformNameStartsWith(s, "GetX")
  }

  "SCollectionWithSideOutput" should "support custom transform name" in {
    val sc = ScioContext()
    val p1 = sc.parallelize(Seq("a", "b", "c"))
    val p2 = SideOutput[String]()
    val (main, side) = p1
      .withSideOutputs(p2)
      .withName("MakeSideOutput")
      .map { (x, s) => s.output(p2, x + "2"); x + "1" }
    val sideOut = side(p2)
    assertTransformNameStartsWith(main, "MakeSideOutput")
    assertTransformNameStartsWith(sideOut, "MakeSideOutput")
  }

  "WindowedSCollection" should "support custom transform name" in {
    val sc = ScioContext()
    val p = sc
      .parallelize(Seq(1, 2, 3, 4, 5))
      .toWindowed
      .withName("Triple")
      .map(x => x.withValue(x.value * 3))
    assertTransformNameStartsWith(p, "Triple")
  }

  "Joins" should "support custom transform names" in {
    val sc = ScioContext()
    val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
    val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
    val inner = p1.withName("inner").join(p2)
    val left = p1.withName("left").leftOuterJoin(p2)
    val right = p1.withName("right").rightOuterJoin(p2)
    val full = p1.withName("full").fullOuterJoin(p2)
    assertTransformNameStartsWith(inner, "inner")
    assertTransformNameStartsWith(left, "left")
    assertTransformNameStartsWith(right, "right")
    assertTransformNameStartsWith(full, "full")
  }

  "MultiJoin" should "support custom transform name" in {
    val sc = ScioContext()
    val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
    val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
    val p = MultiJoin.withName("JoinEm").left(p1, p2)
    assertTransformNameStartsWith(p, "JoinEm")
  }

  "Duplicate transform name" should "have number to make unique" in {
    val sc = ScioContext()
    val p1 = sc
      .parallelize(1 to 5)
      .withName("MyTransform")
      .map(_ * 2)
    val p2 = p1
      .withName("MyTransform")
      .map(_ * 3)
    val p3 = p1
      .withName("MyTransform")
      .map(_ * 4)
    assertTransformNameStartsWith(p1, "MyTransform")
    assertTransformNameStartsWith(p2, "MyTransform2")
    assertTransformNameStartsWith(p3, "MyTransform3")
  }

  "TransformNameable" should "prevent repeated calls to .withName" in {
    val e = the[IllegalArgumentException] thrownBy {
      val sc = ScioContext()
      sc.parallelize(1 to 5)
        .withName("Double")
        .withName("DoubleMap")
        .map(_ * 2)
    }

    val msg = "requirement failed: withName() has already been used to set 'Double' as " +
      "the name for the next transform."
    e should have message msg
  }

  it should "not generate duplicated names" in {
    import com.spotify.scio.io.TextIO
    JobTest[SimpleJob.type]
      .args("--output=top.txt", "--stableUniqueNames=ERROR")
      .output(TextIO("top.txt"))(_ => ())
      .run()
  }

  it should "contain file:line only on outer transform" in {
    val sc = ScioContext()
    val p = sc.parallelize(1 to 5).transform(_.transform(_.map(_ + 1)))
    assertTransformNameStartsWith(
      p,
      """transform\@\{NamedTransformTest\.scala:\d*\}:\d*/transform:\d*/map:\d*"""
    )
  }

  it should "support fall back to default transform names" in {
    val sc = ScioContext()
    val defaultName = sc.tfName(default = Some("default"))
    defaultName should be("default")

    val userNamed = sc.withName("UserNamed").tfName(default = Some("default"))
    userNamed should be("UserNamed")
  }
}
