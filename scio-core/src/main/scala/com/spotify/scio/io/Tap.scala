/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.io

import java.util.UUID

import com.spotify.scio.coders.{AvroBytesUtil, Coder, CoderMaterializer}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.beam.sdk.io.AvroIO
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

/**
 * Placeholder to an external data set that can either be load into memory as an iterator or
 * opened in a new [[ScioContext]] as an [[com.spotify.scio.values.SCollection SCollection]].
 */
trait Tap[T] extends Serializable { self =>

  /** Parent of this Tap before [[map]]. */
  val parent: Option[Tap[_]] = None

  /** Read data set into memory. */
  def value: Iterator[T]

  /** Open data set as an [[com.spotify.scio.values.SCollection SCollection]]. */
  def open(sc: ScioContext): SCollection[T]

  /** Map items from `T` to `U`. */
  def map[U: Coder](f: T => U): Tap[U] = new Tap[U] {

    /** Parent of this Tap before [[map]]. */
    override val parent: Option[Tap[_]] = Option(self)

    /** Read data set into memory. */
    override def value: Iterator[U] = self.value.map(f)

    /** Open data set as an [[com.spotify.scio.values.SCollection SCollection]]. */
    override def open(sc: ScioContext): SCollection[U] = self.open(sc).map(f)
  }
}

case object EmptyTap extends Tap[Nothing] {
  implicit def nothingCoder: Coder[Nothing] = Coder.nothingCoder
  override def value: Iterator[Nothing] = Iterator.empty
  override def open(sc: ScioContext): SCollection[Nothing] = sc.empty[Nothing]()
}

case class UnsupportedTap[T](msg: String) extends Tap[T] {
  override def value: Iterator[T] = throw new UnsupportedOperationException(msg)
  override def open(sc: ScioContext): SCollection[T] = throw new UnsupportedOperationException(msg)
}

/** Tap for text files on local file system or GCS. */
final case class TextTap(path: String) extends Tap[String] {
  override def value: Iterator[String] = FileStorage(path).textFile

  override def open(sc: ScioContext): SCollection[String] = sc.textFile(path)
}

final private[scio] class InMemoryTap[T: Coder] extends Tap[T] {
  private[scio] val id: String = UUID.randomUUID().toString
  override def value: Iterator[T] = InMemorySink.get(id).iterator
  override def open(sc: ScioContext): SCollection[T] =
    sc.parallelize[T](InMemorySink.get(id))
}

private[scio] class MaterializeTap[T: Coder] private (val path: String, coder: BCoder[T])
    extends Tap[T] {
  private val _path = ScioUtil.addPartSuffix(path)

  override def value: Iterator[T] = {
    val storage = FileStorage(_path)

    if (storage.isDone) {
      storage
        .avroFile[GenericRecord](AvroBytesUtil.schema)
        .map(AvroBytesUtil.decode(coder, _))
    } else {
      throw new RuntimeException(
        "Tap failed to materialize to filesystem. Did you " +
          "call .materialize before the ScioContext was run?"
      )
    }
  }

  private def dofn =
    new DoFn[GenericRecord, T] {
      @ProcessElement
      private[scio] def processElement(c: DoFn[GenericRecord, T]#ProcessContext): Unit =
        c.output(AvroBytesUtil.decode(coder, c.element()))
    }

  override def open(sc: ScioContext): SCollection[T] = sc.requireNotClosed {
    val read = AvroIO.readGenericRecords(AvroBytesUtil.schema).from(_path)
    sc.applyTransform(read).parDo(dofn)
  }
}

object MaterializeTap {
  def apply[T: Coder](path: String, context: ScioContext): MaterializeTap[T] =
    new MaterializeTap(path, CoderMaterializer.beam(context, Coder[T]))
}

final case class ClosedTap[T] private[scio] (private[scio] val underlying: Tap[T]) {

  /**
   * Get access to the underlying Tap. The ScioContext has to be ran before.
   * An instance of ScioResult is returned by ScioContext after the context is run.
   * @see ScioContext.run
   */
  def get(result: ScioResult): Tap[T] = result.tap(this)
}
