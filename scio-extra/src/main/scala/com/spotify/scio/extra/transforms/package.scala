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
import java.net.URI
import java.nio.file.Path

import com.spotify.scio.util._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.{TupleTag, TupleTagList}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * Main package for transforms APIs. Import all.
 */
package object transforms {

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with
   * [[java.net.URI URI]] methods.
   */
  implicit class URISCollection(val self: SCollection[URI]) extends AnyVal {
    /**
     * Download [[java.net.URI URI]] elements and process as local [[java.nio.file.Path Path]]s.
     * @param batchSize batch size when downloading files
     * @param keep keep downloaded files after processing
     */
    def mapFile[T: ClassTag](f: Path => T,
                             batchSize: Int = 10,
                             keep: Boolean = false): SCollection[T] =
      self.applyTransform(ParDo.of(new FileDownloadDoFn[T](
        RemoteFileUtil.create(self.context.options),
        Functions.serializableFn(f),
        batchSize, keep)))

    /**
     * Download [[java.net.URI URI]] elements and process as local [[java.nio.file.Path Path]]s.
     * @param batchSize batch size when downloading files
     * @param keep keep downloaded files after processing
     */
    def flatMapFile[T: ClassTag](f: Path => TraversableOnce[T],
                                 batchSize: Int = 10,
                                 keep: Boolean = false): SCollection[T] =
      self
        .applyTransform(ParDo.of(new FileDownloadDoFn[TraversableOnce[T]](
          RemoteFileUtil.create(self.context.options),
          Functions.serializableFn(f),
          batchSize, keep)))
        .flatMap(identity)

  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with pipe methods.
   */
  implicit class PipeSCollection(val self: SCollection[String]) extends AnyVal {

    /**
     * Pipe elements through an external command via StdIn & StdOut.
     * @param command the command to call
     * @param environment environment variables
     * @param dir the working directory of the subprocess
     * @param setupCmds setup commands to be run before processing
     * @param teardownCmds tear down commands to be run after processing
     */
    def pipe(command: String,
             environment: Map[String, String] = null,
             dir: File = null,
             setupCmds: Seq[String] = null,
             teardownCmds: Seq[String] = null): SCollection[String] = {
      val env = if (environment == null) null else environment.asJava
      val sCmds = if (setupCmds == null) null else setupCmds.asJava
      val tCmds = if (teardownCmds == null) null else teardownCmds.asJava
      self.applyTransform(ParDo.of(new PipeDoFn(command, env, dir, sCmds, tCmds)))
    }

    /**
     * Pipe elements through an external command via StdIn & StdOut.
     * @param cmdArray array containing the command to call and its arguments
     * @param environment environment variables
     * @param dir the working directory of the subprocess
     * @param setupCmds setup commands to be run before processing
     * @param teardownCmds tear down commands to be run after processing
     */
    def pipe(cmdArray: Array[String],
             environment: Map[String, String],
             dir: File,
             setupCmds: Seq[Array[String]],
             teardownCmds: Seq[Array[String]]): SCollection[String] = {
      val env = if (environment == null) null else environment.asJava
      val sCmds = if (setupCmds == null) null else setupCmds.asJava
      val tCmds = if (teardownCmds == null) null else teardownCmds.asJava
      self.applyTransform(ParDo.of(new PipeDoFn(cmdArray, env, dir, sCmds, tCmds)))
    }

  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with specialized
   * versions of flatMap.
   */
  implicit class SpecializedFlatMapSCollection[T](val self: SCollection[T]) extends AnyVal {

    /**
     * Latency optimized flavor of
     * [[com.spotify.scio.values.SCollection.flatMap SCollection.flatMap]], it returns a new
     * SCollection by first applying a function to all elements of this SCollection, and then
     * flattening the results. If function throws an exception, instead of retrying, faulty element
     * goes into given error side output.
     *
     * @group transform
     */
    def safeFlatMap[U: ClassTag](f: T => TraversableOnce[U])
    : (SCollection[U], SCollection[(T, Throwable)]) = {
      val (mainTag, errorTag) = (new TupleTag[U], new TupleTag[(T, Throwable)])
      val doFn = new NamedDoFn[T, U] {
        val g = ClosureCleaner(f) // defeat closure
        @ProcessElement
        private[scio] def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
          val i = try {
            g(c.element()).toIterator
          } catch {
            case e: Throwable =>
              c.output(errorTag, (c.element(), e))
              Iterator.empty
          }
          while (i.hasNext) c.output(i.next())
        }
      }
      val tuple = self.applyInternal(
        ParDo.of(doFn).withOutputTags(mainTag, TupleTagList.of(errorTag)))
      val main = tuple.get(mainTag).setCoder(self.getCoder[U])
      val errorPipe = tuple.get(errorTag).setCoder(self.getCoder[(T, Throwable)])
      (self.context.wrap(main), self.context.wrap(errorPipe))
    }

    //TODO(rav): resilientFlatMap
  }

}
