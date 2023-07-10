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
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.MaterializeTap.materializeReader
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.beam.sdk.coders.{ByteArrayCoder, Coder => BCoder, CoderException}
import org.apache.beam.sdk.util.CoderUtils

import java.io.{EOFException, InputStream}

/**
 * Placeholder to an external data set that can either be load into memory as an iterator or opened
 * in a new [[ScioContext]] as an [[com.spotify.scio.values.SCollection SCollection]].
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

final case class UnsupportedTap[T](msg: String) extends Tap[T] {
  override def value: Iterator[T] = throw new UnsupportedOperationException(msg)
  override def open(sc: ScioContext): SCollection[T] = throw new UnsupportedOperationException(msg)
}

/** Tap for text files on local file system or GCS. */
final case class TextTap(path: String, params: TextIO.ReadParam) extends Tap[String] {
  override def value: Iterator[String] =
    FileStorage(path, params.suffix).textFile

  override def open(sc: ScioContext): SCollection[String] =
    sc.read(TextIO(path))(params)
}

final private[scio] class InMemoryTap[T: Coder] extends Tap[T] {
  private[scio] val id: String = UUID.randomUUID().toString
  override def value: Iterator[T] = InMemorySink.get(id).iterator
  override def open(sc: ScioContext): SCollection[T] =
    sc.parallelize[T](InMemorySink.get(id))
}

final private[scio] class MaterializeTap[T: Coder] private (path: String, coder: BCoder[T])
    extends Tap[T] {

  override def value: Iterator[T] = {
    val storage = FileStorage(path, BinaryIORead.ReadParam.DefaultSuffix)
    if (storage.isDone()) {
      val filePattern = ScioUtil.filePattern(path, BinaryIORead.ReadParam.DefaultSuffix)
      BinaryIO
        .openInputStreamsFor(filePattern)
        .flatMap { is =>
          new Iterator[T] {
            private val reader = materializeReader()
            private val ignoredState = reader.start(is)
            private var rec: Option[Array[Byte]] = None
            read()

            def read(): Unit = {
              try {
                val (_, optRecord) = reader.readRecord(ignoredState, is)
                rec = Option(optRecord).flatMap(x => if (x.isEmpty) None else Some(x))
              } catch {
                case _: EOFException =>
                  rec = None
              }
            }

            override def hasNext: Boolean = rec.isDefined
            override def next(): T = {
              val ret = rec.map(arr => CoderUtils.decodeFromByteArray(coder, arr)).get
              read()
              ret
            }
          }
        }
    } else {
      throw new RuntimeException(
        "Tap failed to materialize to filesystem. Did you " +
          "call .materialize before the ScioContext was run?"
      )
    }
  }

  override def open(sc: ScioContext): SCollection[T] = sc.requireNotClosed {
    sc.binaryFile(path, reader = materializeReader())
      .map(ar => CoderUtils.decodeFromByteArray[T](coder, ar))
  }
}

object MaterializeTap {
  def apply[T: Coder](path: String, context: ScioContext): MaterializeTap[T] =
    new MaterializeTap(path, CoderMaterializer.beam(context, Coder[T]))

  private[scio] def materializeReader(): BinaryIORead.BinaryFileReader = {
    // FIXME `with Serializable` is not very elegant ... make case class
    new BinaryIORead.BinaryFileReader with Serializable {
      private val c: BCoder[Array[Byte]] = ByteArrayCoder.of()
      override type State = Unit
      override def start(is: InputStream): State = ()
      override def readRecord(state: State, is: InputStream): (State, Array[Byte]) =
        ((), c.decode(is))
    }
  }
}

final case class ClosedTap[T] private (private[scio] val underlying: Tap[T]) {

  /**
   * Get access to the underlying Tap. The ScioContext has to be ran before. An instance of
   * ScioResult is returned by ScioContext after the context is run.
   * @see
   *   ScioContext.run
   */
  def get(result: ScioResult): Tap[T] = result.tap(this)
}
