/*
 * Copyright 2024 Spotify AB.
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

import com.github.luben.zstd.ZstdDictTrainer
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.CoderMaterializer
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.util.CoderUtils
import org.slf4j.LoggerFactory

import scala.util.{Random, Try}

case class ZstdDictIO[T](path: String) extends ScioIO[T] {
  private val logger = LoggerFactory.getLogger(this.getClass)

  override type ReadP = Nothing // WriteOnly
  override type WriteP = ZstdDictIO.WriteParam

  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override protected def read(sc: ScioContext, params: Nothing): SCollection[T] =
    throw new UnsupportedOperationException("ZstdDictIO is write-only")

  override protected def write(
    data: SCollection[T],
    params: ZstdDictIO.WriteParam
  ): Tap[Nothing] = {
    val ZstdDictIO.WriteParam(
      zstdDictSizeBytes,
      numElementsForSizeEstimation,
      trainingBytesTarget
    ) = params
    // see https://github.com/facebook/zstd/issues/3769#issuecomment-1730261489
    if (zstdDictSizeBytes > (ZstdDictIO.DictionarySizeMbWarningThreshold * 1024 * 1024)) {
      logger.warn(
        s"Dictionary sizes over ${ZstdDictIO.DictionarySizeMbWarningThreshold}MB are of " +
          s"questionable utility. Consider reducing zstdDictSizeBytes."
      )
    }
    if (numElementsForSizeEstimation <= 0) {
      throw new IllegalArgumentException(
        s"numElementsForSizeEstimation must be positive, found $numElementsForSizeEstimation"
      )
    }
    // training bytes may not exceed 2GiB a.k.a. the max value of an Int
    val trainingBytesTargetActual: Int = Option(trainingBytesTarget).getOrElse {
      val computed =
        Try(Math.multiplyExact(zstdDictSizeBytes, 100)).toOption.getOrElse(Int.MaxValue)
      logger.info(s"No trainingBytesTarget passed, using ${computed} bytes")
      computed
    }
    if (trainingBytesTargetActual <= 0) {
      throw new IllegalArgumentException(
        s"trainingBytesTarget must be positive, found $trainingBytesTargetActual"
      )
    }

    val beamCoder = CoderMaterializer.beam(data.context, data.coder)
    def toBytes(v: T): Array[Byte] = CoderUtils.encodeToByteArray(beamCoder, v)

    data
      .transform("Create Zstd Dictionary") { scoll =>
        // estimate the sample rate we need by examining numElementsForSizeEstimation elements
        val streamsCntSI = scoll.count.asSingletonSideInput(0L)
        val sampleRateSI = scoll
          .take(numElementsForSizeEstimation)
          .map(v => toBytes(v).length)
          .sum
          .withSideInputs(streamsCntSI)
          .map { case (totalSize, ctx) =>
            val avgSize = totalSize / numElementsForSizeEstimation
            val targetNumElements = trainingBytesTargetActual / avgSize
            val sampleRate = targetNumElements.toDouble / ctx(streamsCntSI)
            logger.info(s"Computed sample rate for Zstd dictionary: ${sampleRate}")
            sampleRate
          }
          .toSCollection
          .asSingletonSideInput

        scoll
          .withSideInputs(sampleRateSI)
          .flatMap {
            case (s, ctx) if new Random().nextDouble() <= ctx(sampleRateSI) =>
              Some(toBytes(s))
            case _ => None
          }
          .toSCollection
          .keyBy(_ => ())
          .groupByKey
          .map { case (_, elements) =>
            val zstdSampleSize = {
              val sum = elements.map(_.length.toLong).sum
              if (sum > Int.MaxValue.toLong) Int.MaxValue else sum.toInt
            }
            logger.info(s"Training set size for for Zstd dictionary: ${zstdSampleSize}")
            val trainer = new ZstdDictTrainer(zstdSampleSize, zstdDictSizeBytes)
            elements.foreach(trainer.addSample)
            trainer.trainSamples()
          }
      }
      .withName("Save Zstd Dictionary")
      .saveAsBinaryFile(path)
      .underlying
  }

  override def tap(read: Nothing): Tap[tapT.T] = EmptyTap
}

object ZstdDictIO {
  val DictionarySizeMbWarningThreshold: Int = 10

  final case class WriteParam(
    zstdDictSizeBytes: Int = WriteParam.DefaultZstdDictSizeBytes,
    numElementsForSizeEstimation: Long = WriteParam.DefaultNumElementsForSizeEstimation,
    trainingBytesTarget: Int = WriteParam.DefaultTrainingBytesTarget
  )

  object WriteParam {
    val DefaultZstdDictSizeBytes: Int = 110 * 1024
    val DefaultNumElementsForSizeEstimation: Long = 100L
    val DefaultTrainingBytesTarget: Int = null.asInstanceOf[Int]
  }
}
