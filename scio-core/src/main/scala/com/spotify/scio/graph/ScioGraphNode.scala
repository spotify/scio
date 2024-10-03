package com.spotify.scio.graph

import com.spotify.scio.values.SCollection

import scala.reflect.ClassTag

private[scio] case class ScioGraphNode(
  name: String,
  `type`: String,
  io: Option[String],
  dataClass: Class[_],
  schemaPath: Option[String],
  sources: List[ScioGraphNode],
  properties: Map[String, Any]
)

object NodeType {
  val Parallelize: String = "Parallelize"
  val Transform: String = "Transform"
  val UnionAll: String = "UnionAll"
  val FlatMap: String = "FlatMap"
}

object NodeIO {
  val Text: String = "Text"
  val CustomInput: String = "CustomInput"
}

object ScioGraphNode {
  def parallelize[T](implicit ct: ClassTag[T]) =
    node[T](null, NodeType.Parallelize, List())

  def node[T](
    name: String,
    `type`: String,
    sources: List[SCollection[_]],
    properties: Map[String, Any] = Map.empty
  )(implicit
    ct: ClassTag[T]
  ): ScioGraphNode = {
    ScioGraphNode(
      name,
      `type`,
      None,
      ct.runtimeClass,
      None,
      sources.map(_.step),
      properties
    )
  }

  def read[T](name: String, io: String, properties: Map[String, Any] = Map.empty)(implicit
    ct: ClassTag[T]
  ): ScioGraphNode = {
    ScioGraphNode(
      name,
      "read",
      Some(io),
      ct.runtimeClass,
      None,
      List(),
      properties
    )
  }

  def write[T](
    name: String,
    io: String,
    source: SCollection[_],
    properties: Map[String, Any] = Map.empty
  )(implicit
    ct: ClassTag[T]
  ): ScioGraphNode = {
    ScioGraphNode(
      name,
      "write",
      Some(io),
      ct.runtimeClass,
      None,
      List(source.step),
      properties
    )
  }
}

//private[scio] case class SingleSourceNode(
//  name: String,
//  source: ScioGraphNode,
//  dataClass: Class[_],
//  schemaPath: Option[String] = None
//) extends ScioGraphNode {
//  override val sources: List[ScioGraphNode] = List(source)
//}
//
//private[scio] case class TransformStep(name: String, sources: List[ScioGraphNode])
//    extends ScioGraphNode
//
//private[scio] object TransformStep {
//  def apply(name: String, source: ScioGraphNode): TransformStep = TransformStep(name, List(source))
//}

// preserve link to an original PTransform?
//private[scio] case class CustomInput(name: String) extends ScioGraphNode {
//  val sources: List[ScioGraphNode] = List.empty
//}

//private[scio] case class UnionAll(name: String, sources: List[ScioGraphNode]) extends ScioGraphNode

//private[scio] object Parallelize extends ScioGraphNode {
//  override val name: String = "parallelize"
//  override val sources: List[ScioGraphNode] = List.empty
//}

//private[scio] case class ReadTextIO(filePattern: String) extends ScioGraphNode {
//  override val name: String = null
//  override val sources: List[ScioGraphNode] = List()
//}
//
//private[scio] case class TestInput(kind: String) extends ScioGraphNode {
//  override val name: String = kind
//  override val sources: List[ScioGraphNode] = List()
//}
//
//private[scio] case class FlatMap(source: ScioGraphNode) extends ScioGraphNode {
//  val name: String = null
//  override val sources: List[ScioGraphNode] = List(source)
//}
