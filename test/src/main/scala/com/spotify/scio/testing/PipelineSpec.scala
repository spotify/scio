package com.spotify.scio.testing

import java.io.File
import java.util.UUID

import com.google.cloud.dataflow.sdk.util.CoderUtils
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.KryoAtomicCoder
import com.spotify.scio.values.SCollection
import org.scalatest.{FlatSpec, Matchers}

import scala.reflect.ClassTag

/**
 * Trait for unit testing pipelines.
 */
trait PipelineSpec extends FlatSpec with Matchers with PCollectionMatcher {

  /**
   * Test pipeline components with a [[ScioContext]].
   * @param fn code that tests the components and verifies the result
   */
  def runWithContext(fn: ScioContext => Unit): Unit = {
    val sc = ScioContext(Array("--testId=PipelineTest"))
    fn(sc)
    sc.close()
  }

  /**
   * Test pipeline components with in-memory data.
   * @param data input data
   * @param fn code that tests the components by feeding input and extracting output
   * @return output data
   */
  def runWithData[T: ClassTag, U: ClassTag](data: Iterable[T])(fn: SCollection[T] => SCollection[U]): Seq[U] = {
    runWithLocalOutput { sc => fn(sc.parallelize(data)) }
  }

  /**
   * Test pipeline components with in-memory data.
   * @param data1 input data
   * @param data2 input data
   * @param fn code that tests the components by feeding input and extracting output
   * @return output data
   */
  def runWithData[T1: ClassTag, T2: ClassTag, U: ClassTag]
  (data1: Iterable[T1], data2: Iterable[T2])
  (fn: (SCollection[T1], SCollection[T2]) => SCollection[U]): Seq[U] = {
    runWithLocalOutput { sc =>
      fn(sc.parallelize(data1), sc.parallelize(data2))
    }
  }

  /**
   * Test pipeline components with in-memory data.
   * @param data1 input data
   * @param data2 input data
   * @param data3 input data
   * @param fn code that tests the components by feeding input and extracting output
   * @return output data
   */
  def runWithData[T1: ClassTag, T2: ClassTag, T3: ClassTag, U: ClassTag]
  (data1: Iterable[T1], data2: Iterable[T2], data3: Iterable[T3])
  (fn: (SCollection[T1], SCollection[T2], SCollection[T3]) => SCollection[U]): Seq[U] = {
    runWithLocalOutput { sc =>
      fn(sc.parallelize(data1), sc.parallelize(data2), sc.parallelize(data3))
    }
  }

  /**
   * Test pipeline components with in-memory data.
   * @param data1 input data
   * @param data2 input data
   * @param data3 input data
   * @param data4 input data
   * @param fn code that tests the components by feeding input and extracting output
   * @return output data
   */
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
