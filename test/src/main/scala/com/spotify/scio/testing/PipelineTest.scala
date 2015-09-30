package com.spotify.scio.testing

import java.io.File
import java.lang.{Iterable => JIterable}
import java.util.UUID

import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.google.common.collect.Lists
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.KryoAtomicCoder
import com.spotify.scio.values.SCollection
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

trait PipelineTest extends FlatSpec with Matchers with PCollectionMatcher {

  def runWithContext(test: ScioContext => Unit): Unit = {
    val sc = ScioContext(Array("--testId=PipelineTest"))

    test(sc)

    sc.close()
  }

  def runWithData[T: ClassTag, U: ClassTag](data: T*)(fn: SCollection[T] => SCollection[U]): Seq[U] = {
    val sc = ScioContext(Array())

    val p = sc.parallelize(data: _*)
    val tmpDir = new File(
      new File(System.getProperty("java.io.tmpdir")),
      "scio-test-" + UUID.randomUUID().toString)

    fn(p).map(encode).saveAsTextFile(tmpDir.getPath, numShards = 1)

    sc.close()

    val tmpFile = new File(tmpDir, "part-00000-of-00001.txt")
    val r = scala.io.Source
      .fromFile(tmpFile)
      .getLines()
      .map(decode[U])
      .toSeq

    tmpFile.delete()
    tmpDir.delete()

    r
  }

  // Value type Iterable[T] is wrapped from Java and fails equality check
  def iterable[T](elems: T*): Iterable[T] = Lists.newArrayList(elems: _*).asInstanceOf[JIterable[T]].asScala

  private def encode[T](obj: T): String = CoderUtils.encodeToBase64(new KryoAtomicCoder(), obj)

  private def decode[T](b64: String): T = CoderUtils.decodeFromBase64(new KryoAtomicCoder, b64).asInstanceOf[T]

}
