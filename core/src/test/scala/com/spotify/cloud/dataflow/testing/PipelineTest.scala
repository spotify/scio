package com.spotify.cloud.dataflow.testing

import java.io.File
import java.lang.{Iterable => JIterable}
import java.util.UUID

import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.google.common.collect.Lists
import com.spotify.cloud.dataflow.DataflowContext
import com.spotify.cloud.dataflow.coders.KryoAtomicCoder
import com.spotify.cloud.dataflow.values.SCollection
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

trait PipelineTest extends FlatSpec with Matchers with PCollectionMatcher {

  def runWithContext(test: DataflowContext => Unit): Unit = {
    val context = DataflowContext(Array("--testId=PipelineTest"))

    test(context)

    context.close()
  }

  def runWithData[T: ClassTag, U: ClassTag](data: T*)(fn: SCollection[T] => SCollection[U]): Seq[U] = {
    val context = DataflowContext(Array())

    val p = context.parallelize(data: _*)
    val tmpFile = new File(
      new File(System.getProperty("java.io.tmpdir")),
      "pipeline-test-" + UUID.randomUUID().toString)

    fn(p).map(encode).saveAsTextFile(tmpFile.getPath, numShards = 1)

    context.close()

    scala.io.Source
      .fromFile(new File(tmpFile, "part-00000-of-00001.txt"))
      .getLines()
      .map(decode[U])
      .toSeq
  }

  // Value type Iterable[T] is wrapped from Java and fails equality check
  def iterable[T](elems: T*): Iterable[T] = Lists.newArrayList(elems: _*).asInstanceOf[JIterable[T]].asScala

  private def encode[T](obj: T): String = CoderUtils.encodeToBase64(new KryoAtomicCoder(), obj)

  private def decode[T](b64: String): T = CoderUtils.decodeFromBase64(new KryoAtomicCoder, b64).asInstanceOf[T]

}
