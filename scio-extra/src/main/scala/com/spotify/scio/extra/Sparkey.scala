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

  private val sparkeyTransform =
    new PTransform[PCollection[SparkeyUrl], PCollectionView[SparkeyUrl]]() {
    override def expand(input: PCollection[SparkeyUrl]): PCollectionView[SparkeyUrl] = {
      input.apply(View.asSingleton())
    }
  }

  case class SparkeyUrl(val url: String)

  implicit class SparkeyScioContext(val self: ScioContext) {
    def sparkeySideInput(url: SparkeyUrl): SideInput[SparkeyReader] = {
      val view = self.parallelize(Seq(url)).applyInternal(sparkeyTransform)
      new SparkeySideInput(view)
    }
  }

  implicit class SCollectionWithSparkeyWriter[K, V](val self: SCollection[(K, V)])
                                                 (implicit ev1: K <:< String, ev2: V <:< String) {
    def asSparkey(url: SparkeyUrl = SparkeyUrl("default.spi")): SCollection[SparkeyUrl] =
      self.transform { in => in.groupBy(_ => ())
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
          SparkeyUrl(indexFile.toString)
        }
    }
  }

  implicit class SparkeySCollection(val self: SCollection[SparkeyUrl]) {
    def asSparkeySideInput(): SideInput[SparkeyReader] = {
      val view = self.applyInternal(sparkeyTransform)
      new SparkeySideInput(view)
    }
  }

  implicit class SCollectionToSparkeySideInput(val self: SCollection[(String, String)]) {
    def asSparkeySideInput(): SideInput[SparkeyReader] = self.asSparkey().asSparkeySideInput()
  }

  implicit class Reader(val self: SparkeyReader) {
    def get(key: String): Option[String] = Option(self.getAsString(key))

    def toStream(): Stream[(String, String)] = self.iterator().asScala.toStream.map { e =>
      (e.getKeyAsString, e.getValueAsString)
    }
  }

  private[scio] class SparkeySideInput(val view: PCollectionView[SparkeyUrl])
    extends SideInput[SparkeyReader] {
    override def get[I, O](context: DoFn[I, O]#ProcessContext): SparkeyReader =
      JSparkey.open(new File(context.sideInput(view).url))
  }

}
