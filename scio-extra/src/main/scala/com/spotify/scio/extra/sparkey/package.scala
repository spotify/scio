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

import java.util.UUID

import com.spotify.scio.ScioContext
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.{SCollection, SideInput}
import com.spotify.sparkey.SparkeyReader
import org.apache.beam.sdk.transforms.{DoFn, View}
import org.apache.beam.sdk.values.PCollectionView
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

package object sparkey {

  implicit class SparkeyScioContext(val self: ScioContext) extends AnyVal {
    def sparkeySideInput(url: SparkeyUri): SideInput[SparkeyReader] = {
      val view = self.parallelize(Seq(url)).applyInternal(View.asSingleton())
      new SparkeySideInput(view)
    }
  }

  implicit class SparkeyPairSCollection(val self: SCollection[(String, String)]) {
    private val logger: Logger = LoggerFactory.getLogger(classOf[SparkeyPairSCollection])

    /**
     * Write the contents of this SCollection as a Sparkey file, either locally or on GCS.
     *
     * @return A singleton SCollection containing the [[SparkeyUri]] of the saved files.
     */
    def asSparkey(uri: SparkeyUri): SCollection[SparkeyUri] = {
      require(!uri.exists, s"Sparkey URI ${uri.basePath} already exists.")
      self.transform { in =>
        in.groupBy(_ => ())
          .map { case (_, iter) =>
            val writer = new SparkeyWriter(uri)
            val it = iter.iterator
            while (it.hasNext) {
              val kv = it.next()
              writer.put(kv._1.toString, kv._2.toString)
            }
            writer.close()
            uri
          }
      }
    }

    /** Write the contents of this SCollection as a Sparkey file using the default URI. */
    def asSparkey: SCollection[SparkeyUri] = {
      val uid = UUID.randomUUID()
      val uri = ScioUtil.tempLocation(self.context.options) + s"/sparkey-$uid"
      logger.info(s"Sparkey URI: $uri")
      this.asSparkey(SparkeyUri(uri, self.context.options))
    }

    /**
     *  Convert this SCollection to a SideInput[SparkeyReader] by first writing to a Sparkey file.
     */
    def asSparkeySideInput: SideInput[SparkeyReader] = self.asSparkey.asSparkeySideInput
  }

  implicit class SparkeySCollection(val self: SCollection[SparkeyUri]) extends AnyVal {

    /**
     * Convert this SCollection to a SideInput[SparkeyReader]. Note that this SCollection should
     * be a singleton containing a [[SparkeyUri]].
     */
    def asSparkeySideInput: SideInput[SparkeyReader] = {
      val view = self.applyInternal(View.asSingleton())
      new SparkeySideInput(view)
    }
  }

  implicit class RichSparkeyReader(val self: SparkeyReader) extends Map[String, String] {
    override def get(key: String): Option[String] = Option(self.getAsString(key))

    override def iterator: Iterator[(String, String)] =
      self.iterator.asScala.map(e => (e.getKeyAsString, e.getValueAsString))

    //scalastyle:off method.name
    override def +[B1 >: String](kv: (String, B1)): Map[String, B1] =
      throw new NotImplementedError("Sparkey-backed map; operation not supported.")
    override def -(key: String): Map[String, String] =
      throw new NotImplementedError("Sparkey-backed map; operation not supported.")
    //scalastyle:on method.name
  }

  private class SparkeySideInput(val view: PCollectionView[SparkeyUri])
    extends SideInput[SparkeyReader] {
    override def get[I, O](context: DoFn[I, O]#ProcessContext): SparkeyReader =
      SparkeyUri(context.sideInput(view).basePath, context.getPipelineOptions).getReader
  }
}
