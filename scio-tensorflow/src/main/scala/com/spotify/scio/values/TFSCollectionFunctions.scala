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

package com.spotify.scio.values

import java.io.IOException
import java.nio.file.Files
import javax.annotation.Nullable

import com.spotify.scio.io.{TFRecordFileTap, TFRecordOptions, TFRecordSink, Tap}
import com.spotify.scio.testing.TFRecordIO
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, Setup, Teardown}
import org.apache.beam.sdk.{io => gio}
import org.slf4j.LoggerFactory
import org.tensorflow._

import scala.concurrent.Future
import scala.reflect.ClassTag

private class PredictDoFn[T, V](graphBytes: DistCache[Array[Byte]],
                                fetchOp: Seq[String],
                                @Nullable config: Array[Byte],
                                inFn: T => Map[String, Tensor],
                                outFn: Map[String, Tensor] => V) extends DoFn[T, V] {
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
      case e: IllegalArgumentException => {
        throw new IOException("Not a valid TensorFlow Graph serialization: " + e.getMessage)
      }
    }
  }

  @ProcessElement
  def process(c: DoFn[T, V]#ProcessContext): Unit = {
    val runner = s.runner()
    import scala.collection.JavaConverters._
    val i = inFn(c.element())
    try {
      i.foreach { case (op, t) => runner.feed(op, t) }
      fetchOp.foreach(runner.fetch)
      val outTensors = runner.run()
      try {
        import scala.collection.breakOut
        c.output(outFn((fetchOp zip outTensors.asScala)(breakOut)))
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

/** Enhanced version of [[SCollection]] with TensorFlow methods. */
class TensorFlowSCollectionFunctions[T: ClassTag](@transient val self: SCollection[T])
  extends Serializable {

  /**
   * Predict/infer/forward-pass on pre-trained GraphDef.
   *
   * @param graphUri URI of pre-trained/saved TensorFlow model
   * @param fetchOps names of [[org.tensorflow.Operation]]s to fetch the results from
   * @param config configuration parameters for the session specified as a serialized
   *               [[org.tensorflow.framework.ConfigProto]] protocol buffer.
   * @param inFn translates input elements of T to map of input-operation -> [[Tensor]]. This method
   *             takes ownership of the [[Tensor]]s.
   * @param outFn translates output of prediction from map of output-operation -> [[Tensor]], to
   *              elements of V. This method takes ownership of the [[Tensor]]s.
   */
  def predict[V: ClassTag](graphUri: String,
                           fetchOps: Seq[String],
                           @Nullable config: Array[Byte] = null)
                          (inFn: T => Map[String, Tensor])
                          (outFn: Map[String, Tensor] => V): SCollection[V] = {
    val graphBytes = self.context.distCache(graphUri)(f => Files.readAllBytes(f.toPath))
    self.parDo(new PredictDoFn[T, V](graphBytes, fetchOps, config, inFn, outFn))
  }
}

class TFRecordSCollectionFunctions[T <: Array[Byte]](val self: SCollection[T]) {

  /**
   * Save this SCollection as a TensorFlow TFRecord file. Note that elements must be of type
   * `Array[Byte]`. The recommended record encoding is [[org.tensorflow.example.Example]] protocol
   * buffers (which contain [[org.tensorflow.example.Features]] as a field) serialized as bytes.
   * @group output
   */
  def saveAsTfRecordFile(path: String,
                         suffix: String = ".tfrecords",
                         tfRecordOptions: TFRecordOptions = TFRecordOptions.writeDefault)
  : Future[Tap[Array[Byte]]] = {
    if (self.context.isTest) {
      self.context.testOut(TFRecordIO(path))(self.asInstanceOf[SCollection[Array[Byte]]])
      self.saveAsInMemoryTap.asInstanceOf[Future[Tap[Array[Byte]]]]
    } else {
      self.asInstanceOf[SCollection[Array[Byte]]].applyInternal(
        gio.Write.to(new TFRecordSink(self.pathWithShards(path), suffix, tfRecordOptions)))
      self.context.makeFuture(TFRecordFileTap(path + "/part-*"))
    }
  }

}
