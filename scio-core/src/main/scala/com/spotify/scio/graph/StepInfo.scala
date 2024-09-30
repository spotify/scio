package com.spotify.scio.graph

private[scio] trait StepInfo {
  val name: String
  val sources: List[StepInfo]
}

private[scio] case class TransformStep(name: String, sources: List[StepInfo]) extends StepInfo

private[scio] object TransformStep {
  def apply(name: String, source: StepInfo): TransformStep = TransformStep(name, List(source))
}

// preserve link to an original PTransform?
private[scio] case class CustomInput(name: String) extends StepInfo {
  val sources: List[StepInfo] = List.empty
}

private[scio] case class UnionAll(name: String, sources: List[StepInfo]) extends StepInfo

private[scio] object Parallelize extends StepInfo {
  override val name: String = "parallelize"
  override val sources: List[StepInfo] = List.empty
}

private[scio] case class ReadTextIO(filePattern: String) extends StepInfo {
  override val name: String = null
  override val sources: List[StepInfo] = List()
}

private[scio] case class TestInput(kind: String) extends StepInfo {
  override val name: String = kind
  override val sources: List[StepInfo] = List()
}

private[scio] case class FlatMap(source: StepInfo) extends StepInfo {
  val name: String = null
  override val sources: List[StepInfo] = List(source)
}
