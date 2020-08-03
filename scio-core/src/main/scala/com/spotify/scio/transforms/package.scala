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

package com.spotify.scio

import java.io.File
import java.net.URI
import java.nio.file.Path

import com.spotify.scio.util._
import com.spotify.scio.coders.{Coder, CoderMaterializer}

import com.spotify.scio.values.SCollection
import com.twitter.chill.ClosureCleaner
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.values.{TupleTag, TupleTagList}
import com.google.common.util.concurrent.ListenableFuture

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Main package for transforms APIs. Import all. */
package object transforms {

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with
   * [[java.net.URI URI]] methods.
   */
  implicit class URISCollection(private val self: SCollection[URI]) extends AnyVal {

    /**
     * Download [[java.net.URI URI]] elements and process as local [[java.nio.file.Path Path]]s.
     * @param batchSize batch size when downloading files
     * @param keep keep downloaded files after processing
     */
    def mapFile[T: Coder](
      f: Path => T,
      batchSize: Int = 10,
      keep: Boolean = false
    ): SCollection[T] =
      self.applyTransform(
        ParDo.of(
          new FileDownloadDoFn[T](
            RemoteFileUtil.create(self.context.options),
            Functions.serializableFn(f),
            batchSize,
            keep
          )
        )
      )

    /**
     * Download [[java.net.URI URI]] elements and process as local [[java.nio.file.Path Path]]s.
     * @param batchSize batch size when downloading files
     * @param keep keep downloaded files after processing
     */
    def flatMapFile[T: Coder](
      f: Path => TraversableOnce[T],
      batchSize: Int = 10,
      keep: Boolean = false
    ): SCollection[T] =
      self
        .applyTransform(
          ParDo.of(
            new FileDownloadDoFn[TraversableOnce[T]](
              RemoteFileUtil.create(self.context.options),
              Functions.serializableFn(f),
              batchSize,
              keep
            )
          )
        )
        .flatMap(identity)
  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with custom
   * parallelism, where `parallelism` is the number of concurrent `DoFn` threads per worker
   * (default to number of CPU cores).
   */
  implicit class CustomParallelismSCollection[T](@transient private val self: SCollection[T])
      extends AnyVal {
    private def parallelCollectFn[U](parallelism: Int)(pfn: PartialFunction[T, U]): DoFn[T, U] =
      new ParallelLimitedFn[T, U](parallelism) {
        val isDefined = ClosureCleaner.clean(pfn.isDefinedAt(_)) // defeat closure
        val g = ClosureCleaner.clean(pfn) // defeat closure
        def parallelProcessElement(c: DoFn[T, U]#ProcessContext): Unit =
          if (isDefined(c.element())) {
            c.output(g(c.element()))
          }
      }

    private def parallelFilterFn(parallelism: Int)(f: T => Boolean): DoFn[T, T] =
      new ParallelLimitedFn[T, T](parallelism) {
        val g = ClosureCleaner.clean(f) // defeat closure
        def parallelProcessElement(c: DoFn[T, T]#ProcessContext): Unit =
          if (g(c.element())) {
            c.output(c.element())
          }
      }

    private def parallelMapFn[U](parallelism: Int)(f: T => U): DoFn[T, U] =
      new ParallelLimitedFn[T, U](parallelism) {
        val g = ClosureCleaner.clean(f) // defeat closure
        def parallelProcessElement(c: DoFn[T, U]#ProcessContext): Unit =
          c.output(g(c.element()))
      }

    private def parallelFlatMapFn[U](parallelism: Int)(f: T => TraversableOnce[U]): DoFn[T, U] =
      new ParallelLimitedFn[T, U](parallelism: Int) {
        val g = ClosureCleaner.clean(f) // defeat closure
        def parallelProcessElement(c: DoFn[T, U]#ProcessContext): Unit = {
          val i = g(c.element()).toIterator
          while (i.hasNext) c.output(i.next())
        }
      }

    /**
     * Return a new SCollection by first applying a function to all elements of
     * this SCollection, and then flattening the results.
     * `parallelism` is the number of concurrent `DoFn`s per worker.
     * @group transform
     */
    def flatMapWithParallelism[U: Coder](
      parallelism: Int
    )(fn: T => TraversableOnce[U]): SCollection[U] =
      self.parDo(parallelFlatMapFn(parallelism)(fn))

    /**
     * Return a new SCollection containing only the elements that satisfy a predicate.
     * `parallelism` is the number of concurrent `DoFn`s per worker.
     * @group transform
     */
    def filterWithParallelism(
      parallelism: Int
    )(fn: T => Boolean)(implicit coder: Coder[T]): SCollection[T] =
      self.parDo(parallelFilterFn(parallelism)(fn))

    /**
     * Return a new SCollection by applying a function to all elements of this SCollection.
     * `parallelism` is the number of concurrent `DoFn`s per worker.
     * @group transform
     */
    def mapWithParallelism[U: Coder](parallelism: Int)(fn: T => U): SCollection[U] =
      self.parDo(parallelMapFn(parallelism)(fn))

    /**
     * Filter the elements for which the given `PartialFunction` is defined, and then map.
     * `parallelism` is the number of concurrent `DoFn`s per worker.
     * @group transform
     */
    def collectWithParallelism[U: Coder](
      parallelism: Int
    )(pfn: PartialFunction[T, U]): SCollection[U] =
      self.parDo(parallelCollectFn(parallelism)(pfn))
  }

  /** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with pipe methods. */
  implicit class PipeSCollection(@transient private val self: SCollection[String]) extends AnyVal {

    /**
     * Pipe elements through an external command via StdIn & StdOut.
     * @param command the command to call
     * @param environment environment variables
     * @param dir the working directory of the subprocess
     * @param setupCmds setup commands to be run before processing
     * @param teardownCmds tear down commands to be run after processing
     */
    def pipe(
      command: String,
      environment: Map[String, String] = null,
      dir: File = null,
      setupCmds: Seq[String] = null,
      teardownCmds: Seq[String] = null
    ): SCollection[String] = {
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
    def pipe(
      cmdArray: Array[String],
      environment: Map[String, String],
      dir: File,
      setupCmds: Seq[Array[String]],
      teardownCmds: Seq[Array[String]]
    ): SCollection[String] = {
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
  implicit class SpecializedFlatMapSCollection[T](@transient private val self: SCollection[T])
      extends AnyVal {

    /**
     * Latency optimized flavor of
     * [[com.spotify.scio.values.SCollection.flatMap SCollection.flatMap]], it returns a new
     * SCollection by first applying a function to all elements of this SCollection, and then
     * flattening the results. If function throws an exception, instead of retrying, faulty element
     * goes into given error side output.
     *
     * @group transform
     */
    def safeFlatMap[U: Coder](
      f: T => TraversableOnce[U]
    )(implicit coder: Coder[T]): (SCollection[U], SCollection[(T, Throwable)]) = {
      val (mainTag, errorTag) = (new TupleTag[U], new TupleTag[(T, Throwable)])
      val doFn = new NamedDoFn[T, U] {
        val g = ClosureCleaner.clean(f) // defeat closure
        @ProcessElement
        private[scio] def processElement(c: DoFn[T, U]#ProcessContext): Unit = {
          val i =
            try {
              g(c.element()).toIterator
            } catch {
              case e: Throwable =>
                c.output(errorTag, (c.element(), e))
                Iterator.empty
            }
          while (i.hasNext) c.output(i.next())
        }
      }
      val tuple =
        self.applyInternal(ParDo.of(doFn).withOutputTags(mainTag, TupleTagList.of(errorTag)))
      val main = tuple
        .get(mainTag)
        .setCoder(CoderMaterializer.beam(self.context, Coder[U]))
      val errorPipe =
        tuple
          .get(errorTag)
          .setCoder(CoderMaterializer.beam(self.context, Coder[(T, Throwable)]))
      (self.context.wrap(main), self.context.wrap(errorPipe))
    }

    //TODO(rav): resilientFlatMap
  }

  /** Enhanced version of `AsyncLookupDoFn.Try` with convenience methods. */
  implicit class RichAsyncLookupDoFnTry[A](private val self: BaseAsyncLookupDoFn.Try[A])
      extends AnyVal {

    /** Convert this `AsyncLookupDoFn.Try` to a Scala `Try`. */
    def asScala: Try[A] =
      if (self.isSuccess) Success(self.get()) else Failure(self.getException)
  }
}
