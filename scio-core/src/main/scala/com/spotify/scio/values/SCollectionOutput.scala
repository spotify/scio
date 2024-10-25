package com.spotify.scio.values;

import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.{PInput, POutput, PValue, TupleTag}

import java.util;

case class SCollectionOutput[T](scioCollection: SCollection[T]) extends POutput {
  override def getPipeline: Pipeline = scioCollection.internal.getPipeline

  override def expand(): util.Map[TupleTag[_], PValue] = scioCollection.internal.expand()

  override def finishSpecifyingOutput(
    transformName: String,
    input: PInput,
    transform: PTransform[_, _]
  ): Unit = scioCollection.internal.finishSpecifyingOutput(transformName, input, transform)
}
