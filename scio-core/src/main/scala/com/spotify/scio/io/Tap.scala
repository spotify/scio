/*
 * Copyright 2016 Spotify AB.
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

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.AvroBytesUtil
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import com.twitter.chill.Externalizer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.reflect.ClassTag

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
  def map[U: ClassTag](f: T => U): Tap[U] = new Tap[U] {

    /** Parent of this Tap before [[map]]. */
    override val parent: Option[Tap[_]] = Option(self)

    /** Read data set into memory. */
    override def value: Iterator[U] = self.value.map(f)

    /** Open data set as an [[com.spotify.scio.values.SCollection SCollection]]. */
    override def open(sc: ScioContext): SCollection[U] = self.open(sc).map(f)
  }

}

/** Tap for text files on local file system or GCS. */
case class TextTap(path: String) extends Tap[String] {
  override def value: Iterator[String] = FileStorage(path).textFile
  override def open(sc: ScioContext): SCollection[String] = sc.textFile(path)
}

/**
 * Tap for Avro files on local file system or GCS.
 * @param schema must be not null if `T` is of type
 *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
 */
case class AvroTap[T: ClassTag](path: String,
                                @transient private val schema: Schema = null) extends Tap[T] {
  private lazy val s = Externalizer(schema)
  override def value: Iterator[T] = FileStorage(path).avroFile(s.get)
  override def open(sc: ScioContext): SCollection[T] = sc.avroFile[T](path, s.get)
}

/**
 * Tap for object files on local file system or GCS. Note that serialization is not guaranteed to
 * be compatible across Scio releases.
 */
case class ObjectFileTap[T: ClassTag](path: String) extends Tap[T] {
  override def value: Iterator[T] = {
    val elemCoder = ScioUtil.getScalaCoder[T]
    FileStorage(path).avroFile[GenericRecord](AvroBytesUtil.schema).map { r =>
      AvroBytesUtil.decode(elemCoder, r)
    }
  }
  override def open(sc: ScioContext): SCollection[T] = sc.objectFile(path)
}

private[scio] class InMemoryTap[T: ClassTag] extends Tap[T] {
  private[scio] val id: String = UUID.randomUUID().toString
  override def value: Iterator[T] = InMemorySink.get(id).iterator
  override def open(sc: ScioContext): SCollection[T] =
    sc.parallelize[T](InMemorySink.get(id))
}
