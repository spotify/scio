package com.spotify.scio.testing

import org.apache.beam.sdk.Pipeline.PipelineVisitor
import org.apache.beam.sdk.Pipeline.PipelineVisitor.CompositeBehavior
import org.apache.beam.sdk.runners.{PTransformMatcher, TransformHierarchy}
import org.apache.beam.sdk.transforms.PTransform

import scala.collection.mutable

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
    if (freedNodes.contains(node.getEnclosingNode)) freedNodes += node
    else if (matcher.matches(node.toAppliedPTransform(getPipeline))) {
      matches += node
      freedNodes += node
    }
  }

  def result(): Iterable[PTransform[_, _]] = matches.map(_.getTransform).toList
}
