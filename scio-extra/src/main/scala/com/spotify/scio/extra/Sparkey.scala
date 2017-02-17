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

import com.spotify.scio.ScioContext
import com.spotify.scio.values.{SCollection, SideInput}
import com.spotify.sparkey.{CompressionType, SparkeyReader, Sparkey => JSparkey}
import org.apache.beam.sdk.transforms.{DoFn, PTransform, View}
import org.apache.beam.sdk.values.{PCollection, PCollectionView}

import scala.collection.JavaConverters._

object Sparkey {

  implicit class PairSCollectionWithSparkey[K, V](val self: SCollection[(K, V)])
                                                 (implicit ev1: K <:< String, ev2: V <:< String) {
    def asSparkeySideInput: SideInput[SparkeyReader] = {
      val f = self.groupBy(_ => ())
        .map { case (_, iter) =>
          val indexFile = new File("test.spi")
          val writer = JSparkey.createNew(indexFile, CompressionType.NONE, 512);
          val it = iter.iterator
          while (it.hasNext) {
            val kv = it.next()
            writer.put(kv._1.toString, kv._2.toString)
          }
          writer.flush()
          writer.writeHash()
          writer.close()
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

  implicit class Reader(val self: SparkeyReader) {
    def get(key: String): Option[String] = Option(self.getAsString(key))

    def toStream(): Stream[(String, String)] = self.iterator().asScala.toStream.map { e =>
      (e.getKeyAsString, e.getValueAsString)
    }
  }

  implicit class SparkeyScioContext(val self: ScioContext) {
    def sparkeySideInput(indexFile: String): SideInput[Map[String, String]] = ???
  }

  private[scio] class SparkeySideInput(val view: PCollectionView[String])
    extends SideInput[SparkeyReader] {
    override def get[I, O](context: DoFn[I, O]#ProcessContext): SparkeyReader =
      JSparkey.open(new File(context.sideInput(view)))
  }

}
