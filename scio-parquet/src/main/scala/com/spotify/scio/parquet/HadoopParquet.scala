/*
 * Copyright 2026 Spotify AB.
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

package com.spotify.scio.parquet

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import com.twitter.chill.ClosureCleaner
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, SimpleFunction}
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.AtomicBoolean
import scala.reflect.ClassTag

private[parquet] object HadoopParquet {

  /** Read data from Hadoop format using HadoopFormatIO. */
  def readHadoopFormatIO[A: ClassTag, T: ClassTag](
    sc: ScioContext,
    conf: Configuration,
    projectionFn: Option[A => T],
    skipValueClone: Option[Boolean]
  )(implicit coder: Coder[T]): SCollection[T] = {
    val inputType = ScioUtil.classOf[A]
    val outputType = ScioUtil.classOf[T]
    val bcoder = CoderMaterializer.beam(sc, Coder[T])

    val hadoop = HadoopFormatIO
      .read[java.lang.Boolean, T]()
      // Hadoop input always emit key-value, and `Void` causes NPE in Beam coder
      .withKeyTranslation(new SimpleFunction[Void, java.lang.Boolean]() {
        override def apply(input: Void): java.lang.Boolean = true
      })

    val withSkipClone = skipValueClone.fold(hadoop)(skip => hadoop.withSkipValueClone(skip))

    val withValueTranslation = projectionFn.fold {
      withSkipClone.withValueTranslation(
        new SimpleFunction[T, T]() {
          override def apply(input: T): T = input

          override def getInputTypeDescriptor: TypeDescriptor[T] = TypeDescriptor.of(outputType)

          override def getOutputTypeDescriptor = TypeDescriptor.of(outputType)
        },
        bcoder
      )
    } { fn =>
      val g = ClosureCleaner.clean(fn) // defeat closure
      withSkipClone.withValueTranslation(
        new SimpleFunction[A, T]() {
          // Workaround for incomplete Avro objects
          // `SCollection#map` might throw NPE on incomplete Avro objects when the runner tries
          // to serialized them. Lifting the mapping function here fixes the problem.
          override def apply(input: A): T = g(input)

          override def getInputTypeDescriptor = TypeDescriptor.of(inputType)

          override def getOutputTypeDescriptor = TypeDescriptor.of(outputType)
        },
        bcoder
      )
    }

    sc.applyTransform(withValueTranslation.withConfiguration(conf)).map(_.getValue)
  }
}

private[parquet] object LineageReportDoFn {
  // Atomic flag to ensure lineage is reported only once per JVM
  private val lineageReported = new AtomicBoolean(false)
}

/**
 * DoFn that reports directory-level source lineage for legacy Parquet reads.
 *
 * @param filePattern
 *   The file pattern or path to report lineage for
 */
private[parquet] class LineageReportDoFn[T](filePattern: String) extends DoFn[T, T] {

  @transient
  private lazy val logger = LoggerFactory.getLogger(this.getClass)

  @ProcessElement
  def processElement(@DoFn.Element element: T, out: DoFn.OutputReceiver[T]): Unit = {
    if (LineageReportDoFn.lineageReported.compareAndSet(false, true)) {
      try {
        val isDirectory = filePattern.endsWith("/")
        val resourceId = FileSystems.matchNewResource(filePattern, isDirectory)
        val directory = resourceId.getCurrentDirectory
        FileSystems.reportSourceLineage(directory)
      } catch {
        case e: Exception =>
          logger.warn(
            s"Error when reporting lineage for pattern: $filePattern",
            e
          )
      }
    }
    out.output(element)
  }
}
