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

package com.spotify.scio.extra

import java.io.File

import com.spotify.scio.values.{SCollection, SideInput}
import com.spotify.sparkey.{CompressionType, Sparkey => JSparkey}
import org.apache.beam.sdk.transforms.{DoFn, PTransform, View}
import org.apache.beam.sdk.values.{PCollection, PCollectionView}

object Sparkey {

  implicit class PairSCollectionWithSparkey[K, V](val self: SCollection[(K, V)])
                                                 (implicit ev1: K <:< String, ev2: V <:< String) {
    def asSparkeySideInput: SideInput[Map[String, String]] = {
      val f = self.groupBy(_ => ()).values
        .map(_.map(kv => (kv._1.toString, kv._2.toString))).map { iter =>
        val indexFile = new File("test.spi")
        val writer = JSparkey.createNew(indexFile, CompressionType.NONE, 512);
        iter.foreach { kv =>
          writer.put(kv._1, kv._2)
          writer.flush();
        }
        writer.writeHash();
        writer.close();
        indexFile.toString
      }
      val o = f.applyInternal(
        new PTransform[PCollection[String], PCollectionView[String]]() {
          override def expand(input: PCollection[String]): PCollectionView[String] = {
            input.apply(View.asSingleton())
          }
        }
      )

      new SparkeySideInput(o)
    }
  }

  private[scio] class SparkeySideInput(val view: PCollectionView[String])
    extends SideInput[Map[String, String]] {
    override def get[I, O](context: DoFn[I, O]#ProcessContext): Map[String, String] =
      new SparkeySideInputMap(context.sideInput(view))
  }

  private class SparkeySideInputMap(indexFile: String) extends Map[String, String] {
    val reader = JSparkey.open(new File(indexFile))

    override def get(key: String): Option[String] = Option(reader.getAsString(key))

    override def iterator: Iterator[(String, String)] = new Iterator[(String, String)] {
      val delegate = reader.iterator()

      override def hasNext: Boolean = delegate.hasNext

      override def next(): (String, String) = {
        val entry = delegate.next()
        (entry.getKeyAsString, entry.getValueAsString)
      }
    }

    // scalastyle:off method.name
    override def +[B1 >: String](kv: (String, B1)): Map[String, B1] = ???

    override def -(key: String): Map[String, String] = ???
    // scalastyle:on method.name
  }

}
