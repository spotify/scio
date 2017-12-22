/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.tensorflow

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.file.Files
import javax.annotation.Nullable

import com.google.common.base.Charsets
import com.spotify.scio.io.{Tap, TextTap}
import com.spotify.scio.testing.TextIO
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.{DistCache, SCollection}
import org.apache.beam.sdk.io.{Compression, FileSystems}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, Setup, Teardown}
import org.apache.beam.sdk.util.MimeTypes
import org.apache.beam.sdk.{io => gio}
import org.slf4j.LoggerFactory
import org.tensorflow._
import org.tensorflow.example.Example

import scala.concurrent.Future
import scala.reflect.ClassTag

private class PredictDoFn[T, V](graphBytes: DistCache[Array[Byte]],
                                fetchOp: Seq[String],
                                @Nullable config: Array[Byte],
                                inFn: T => Map[String, Tensor],
                                outFn: (T, Map[String, Tensor]) => V) extends DoFn[T, V] {
  @transient private lazy val log = LoggerFactory.getLogger(this.getClass)
  @transient private var g: Graph = _
  @transient private var s: Session = _

  @Setup
  def setup(): Unit = {
    log.info("Loading TensorFlow graph")
    g = new Graph()
    s = new Session(g, config)
    val loadStart = System.currentTimeMillis
    try {
      g.importGraphDef(graphBytes())
      log.info(s"TensorFlow graph loaded in ${System.currentTimeMillis - loadStart} ms")
    } catch {
      case e: IllegalArgumentException =>
        throw new IOException("Not a valid TensorFlow Graph serialization: " + e.getMessage)
    }
  }

  @ProcessElement
  def process(c: DoFn[T, V]#ProcessContext): Unit = {
    val runner = s.runner()
    import scala.collection.JavaConverters._
    val input = c.element()
    val i = inFn(input)
    try {
      i.foreach { case (op, t) => runner.feed(op, t) }
      fetchOp.foreach(runner.fetch)
      val outTensors = runner.run()
      try {
        import scala.collection.breakOut
        c.output(outFn(input, (fetchOp zip outTensors.asScala) (breakOut)))
      } finally {
        log.debug("Closing down output tensors")
        outTensors.asScala.foreach(_.close())
      }
    } finally {
      log.debug("Closing down input tensors")
      i.foreach { case (_, t) => t.close() }
    }
  }

  @Teardown
  def teardown(): Unit = {
    log.info(s"Closing down predict DoFn $this")
    try {
      if (s != null) {
        log.info(s"Closing down TensorFlow session $s")
        s.close()
        s = null
      }
    } finally {
      if (g != null) {
        log.info(s"Closing down TensorFlow graph $s")
        g.close()
        g = null
      }
    }
  }
}

/**
  * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with TensorFlow methods.
  */
class TensorFlowSCollectionFunctions[T: ClassTag](@transient val self: SCollection[T])
  extends Serializable {

  /**
    * Predict/infer/forward-pass on pre-trained GraphDef.
    *
    * @param graphUri URI of pre-trained/saved TensorFlow model
    * @param fetchOps names of [[org.tensorflow.Operation]]s to fetch the results from
    * @param config   configuration parameters for the session specified as a serialized
    *                 `org.tensorflow.framework.ConfigProto` protocol buffer.
    * @param inFn     translates input elements of T to map of input-operation ->
    *                 [[org.tensorflow.Tensor Tensor]]. This method takes ownership of the
    *                 [[org.tensorflow.Tensor Tensor]]s.
    * @param outFn    translates output of prediction from map of output-operation ->
    *                 [[org.tensorflow.Tensor Tensor]], to elements of V. This method takes
    *                 ownership of the [[org.tensorflow.Tensor Tensor]]s.
    */
  def predict[V: ClassTag](graphUri: String,
                           fetchOps: Seq[String],
                           config: Array[Byte] = null)
                          (inFn: T => Map[String, Tensor])
                          (outFn: (T, Map[String, Tensor]) => V): SCollection[V] = {
    val graphBytes = self.context.distCache(graphUri)(f => Files.readAllBytes(f.toPath))
    self.parDo(new PredictDoFn[T, V](graphBytes, fetchOps, config, inFn, outFn))
  }
}

class TFExampleSCollectionFunctions[T <: Example](val self: SCollection[T]) {

  /**
    * Save this SCollection of `org.tensorflow.example.Example` as a TensorFlow TFRecord file.
    *
    * @param tFRecordSpec     TF Record description for the Examples, use the
    *                         [[com.spotify.scio.tensorflow.TFRecordSpec]] to define a description.
    * @param tfRecordSpecPath path to save the TF Record description to, by default it will be
    *                         `<PATH>/.tf_record_spec.json`
    * @group output
    */
  def saveAsTfExampleFile(path: String,
                          tFRecordSpec: TFRecordSpec,
                          suffix: String = ".tfrecords",
                          numShards: Int = 0,
                          tfRecordSpecPath: String = null)
                         (implicit ev: T <:< Example)
  : (Future[Tap[Example]], Future[Tap[String]]) = {
    require(tFRecordSpec != null, "TFRecord spec can't be null")
    require(path != null, "Path can't be null")
    val _tfRecordSpecPath =
      Option(tfRecordSpecPath).getOrElse(path.replaceAll("\\/+$", "") + "/_tf_record_spec.json")
    import scala.concurrent.ExecutionContext.Implicits.global

    val tfrs: SCollection[String] = tFRecordSpec match {
      case CaseClassTFRecordSpec(x, c) => self.context.parallelize(Seq(
        TFRecordSpecConfig(1, x, c).asJson))
      case FeatranTFRecordSpec(x, c) => x.map(TFRecordSpecConfig(1, _, c).asJson)
    }

    if (self.context.isTest) {
      self.context.testOut(TextIO(_tfRecordSpecPath))(tfrs)
      self.context.testOut(TFExampleIO(path))(self.asInstanceOf[SCollection[Example]])
      (self.saveAsInMemoryTap.asInstanceOf[Future[Tap[Example]]],
        tfrs.saveAsInMemoryTap.asInstanceOf[Future[Tap[String]]])
    } else {
      tfrs.map { e =>
        val featureSpecResource = FileSystems.matchNewResource(_tfRecordSpecPath, false)
        val writer = FileSystems.create(featureSpecResource, MimeTypes.TEXT)
        try {
          writer.write(ByteBuffer.wrap(s"$e\n".getBytes(Charsets.UTF_8)))
        } finally {
          writer.close()
        }
      }
      val featureSpecFuture = Future(TextTap(_tfRecordSpecPath))
      val r = self.map(_.toByteArray).saveAsTfRecordFile(
        path, suffix, tFRecordSpec.compression, numShards)
      (r.map(_.map(Example.parseFrom)), featureSpecFuture)
    }
  }

}

class TFRecordSCollectionFunctions[T <: Array[Byte]](val self: SCollection[T]) {

  /**
    * Save this SCollection as a TensorFlow TFRecord file. Note that elements must be of type
    * `Array[Byte]`. The recommended record encoding is `org.tensorflow.example.Example` protocol
    * buffers (which contain `org.tensorflow.example.Features` as a field) serialized as bytes.
    *
    * @group output
    */
  def saveAsTfRecordFile(path: String,
                         suffix: String = ".tfrecords",
                         compression: Compression = Compression.UNCOMPRESSED,
                         numShards: Int = 0)
                        (implicit ev: T <:< Array[Byte]): Future[Tap[Array[Byte]]] = {
    if (self.context.isTest) {
      self.context.testOut(TFRecordIO(path))(self.asInstanceOf[SCollection[Array[Byte]]])
      self.saveAsInMemoryTap.asInstanceOf[Future[Tap[Array[Byte]]]]
    } else {
      self.asInstanceOf[SCollection[Array[Byte]]].applyInternal(
        gio.TFRecordIO.write().to(self.pathWithShards(path))
          .withSuffix(suffix)
          .withNumShards(numShards)
          .withCompression(compression))
      self.context.makeFuture(TFRecordFileTap(ScioUtil.addPartSuffix(path)))
    }
  }

}
