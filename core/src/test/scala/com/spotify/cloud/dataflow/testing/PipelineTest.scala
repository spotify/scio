package com.spotify.cloud.dataflow.testing

import java.lang.{Iterable => JIterable}

import com.google.common.collect.Lists
import com.spotify.cloud.dataflow.DataflowContext
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.JavaConverters._

trait PipelineTest extends FlatSpec with Matchers with PCollectionMatcher {

  def runWithContext(test: DataflowContext => Unit): Unit = {
    val context = DataflowContext(Array("--testId=PipelineTest"))

    test(context)

    context.close()
  }

  // Value type Iterable[T] is wrapped from Java and fails equality check
  def iterable[T](elems: T*): Iterable[T] = Lists.newArrayList(elems: _*).asInstanceOf[JIterable[T]].asScala

}
