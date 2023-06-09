/*
 * Copyright 2023 Spotify AB
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

package com.spotify.scio.testing

import org.apache.beam.sdk.Pipeline.PipelineVisitor
import org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior
import org.apache.beam.sdk.runners.{PTransformMatcher, TransformHierarchy}
import org.apache.beam.sdk.transforms.PTransform

import scala.collection.mutable

/**
 * Pipeline visitor collection all matched [[PTransform]]. This can be used in test to make sure the
 * underlying transforms are properly configured
 *
 * @Example
 *   {{{
 *   val sc = ... // the pipeline containing a named transform
 *   val matcher = new EqualNamePTransformMatcher(name)
 *   val finder = new TransformFinder(matcher)
 *   sc.pipeline.traverseTopologically(finder)
 *   val transform = finder.result().head
 *   // check transform configuration in the DisplayData
 *   val displayData = DisplayData.from(transform).asMap().asScala
 *   displayData("config").getValue shouldBe expected
 *   }}}
 */
class TransformFinder(matcher: PTransformMatcher) extends PipelineVisitor.Defaults {

  private val matches = mutable.Set.empty[org.apache.beam.sdk.runners.TransformHierarchy#Node]
  private val freedNodes = mutable.Set.empty[org.apache.beam.sdk.runners.TransformHierarchy#Node]

  override def enterCompositeTransform(
    node: TransformHierarchy#Node
  ): PipelineVisitor.CompositeBehavior = {
    if (!node.isRootNode && freedNodes.contains(node.getEnclosingNode)) {
      // This node will be freed because its parent will be freed.
      freedNodes += node
      return CompositeBehavior.ENTER_TRANSFORM
    }
    if (!node.isRootNode && matcher.matches(node.toAppliedPTransform(getPipeline))) {
      matches += node
      // This node will be freed. When we visit any of its children, they will also be freed
      freedNodes += node
    }
    CompositeBehavior.ENTER_TRANSFORM
  }

  override def visitPrimitiveTransform(node: TransformHierarchy#Node): Unit = {
    if (freedNodes.contains(node.getEnclosingNode)) {
      freedNodes += node
    } else if (matcher.matches(node.toAppliedPTransform(getPipeline))) {
      matches += node
      freedNodes += node
    }
  }

  /** @return Collected matched [[PTransform]] */
  def result(): Iterable[PTransform[_, _]] = matches.map(_.getTransform).toList
}
