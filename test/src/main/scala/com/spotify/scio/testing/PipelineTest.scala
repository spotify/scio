package com.spotify.scio.testing

import java.io.File
import java.util.UUID

import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.KryoAtomicCoder
import com.spotify.scio.values.SCollection
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.ClassTag

trait PipelineTest extends FlatSpec with Matchers with PCollectionMatcher {

  def runWithContext(test: ScioContext => Unit): Unit = {
    val sc = ScioContext(Array("--testId=PipelineTest"))

    test(sc)

    sc.close()
  }

  def runWithData[T: ClassTag, U: ClassTag](data: Iterable[T])(fn: SCollection[T] => SCollection[U]): Seq[U] = {
    runWithLocalOutput { sc => fn(sc.parallelize(data)) }
  }

  def runWithData[T1: ClassTag, T2: ClassTag, U: ClassTag]
  (data1: Iterable[T1], data2: Iterable[T2])
  (fn: (SCollection[T1], SCollection[T2]) => SCollection[U]): Seq[U] = {
    runWithLocalOutput { sc =>
      fn(sc.parallelize(data1), sc.parallelize(data2))
    }
  }

  def runWithData[T1: ClassTag, T2: ClassTag, T3: ClassTag, U: ClassTag]
  (data1: Iterable[T1], data2: Iterable[T2], data3: Iterable[T3])
  (fn: (SCollection[T1], SCollection[T2], SCollection[T3]) => SCollection[U]): Seq[U] = {
    runWithLocalOutput { sc =>
      fn(sc.parallelize(data1), sc.parallelize(data2), sc.parallelize(data3))
    }
  }

  def runWithData[T1: ClassTag, T2: ClassTag, T3: ClassTag, T4: ClassTag, U: ClassTag]
  (data1: Iterable[T1], data2: Iterable[T2], data3: Iterable[T3], data4: Iterable[T4])
  (fn: (SCollection[T1], SCollection[T2], SCollection[T3], SCollection[T4]) => SCollection[U]): Seq[U] = {
    runWithLocalOutput { sc =>
      fn(sc.parallelize(data1), sc.parallelize(data2), sc.parallelize(data3), sc.parallelize(data4))
    }
  }

  private def runWithLocalOutput[U](fn: ScioContext => SCollection[U]): Seq[U] = {
    val sc = ScioContext(Array())

    val tmpDir = new File(
      new File(System.getProperty("java.io.tmpdir")),
      "scio-test-" + UUID.randomUUID().toString)
    fn(sc).map(encode).saveAsTextFile(tmpDir.getPath, numShards = 1)

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

  private def encode[T](obj: T): String = CoderUtils.encodeToBase64(new KryoAtomicCoder(), obj)

  private def decode[T](b64: String): T = CoderUtils.decodeFromBase64(new KryoAtomicCoder, b64).asInstanceOf[T]

}
