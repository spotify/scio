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

import java.net.URI
import java.nio.file.Files

import com.spotify.scio.io.{TFRecordFileTap, TFRecordOptions, TFRecordSink, Tap}
import com.spotify.scio.testing.TFRecordIO
import com.spotify.scio.util.RemoteFileUtil
import org.apache.beam.sdk.{io => gio}
import org.tensorflow.{Graph, SavedModelBundle, Session, Tensor}

import scala.concurrent.Future

/** Enhanced version of [[SCollection]] with Tensorflow methods. */
class TensorSCollectionFunctions(val self: SCollection[Tensor]) extends AnyVal {

  /**
   *
   * @param graphUri
   * @param feedOp
   * @param fetchOp
   * @param config
   * @return
   */
  def predict(graphUri: String,
              feedOp: String,
              fetchOp: Seq[String],
              config: Array[Byte] = null): SCollection[Seq[Tensor]] = {
    import scala.collection.JavaConverters._
    val rfu = RemoteFileUtil.create(self.context.options)
    lazy val g = {
      val p = rfu.download(URI.create(graphUri))
      val g = new Graph()
      g.importGraphDef(Files.readAllBytes(p))
      //TODO: is this gonna keep resources for too long?
      sys.addShutdownHook(g.close())
      g
    }
    lazy val session = {
      val s = new Session(g, config)
      //TODO: is this gonna keep resources for too long?
      sys.addShutdownHook(s.close())
      s
    }
    self.map { t =>
      val r = session.runner().feed(feedOp, t)
      fetchOp.foreach(r.fetch)
      r.run().asScala
    }
  }

}

class TFRecordSCollectionFunctions[T](val self: SCollection[T <:< Array[Byte]]) extends AnyVal {

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
