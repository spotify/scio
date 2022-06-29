package com.spotify.scio.parquet.read

import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.beam.sdk.transforms.{PTransform, ParDo, SerializableFunction}
import org.apache.beam.sdk.values.{PBegin, PCollection}

object ParquetRead {

  def read[T, R](
    readSupportFactory: ReadSupportFactory[T],
    conf: SerializableConfiguration,
    filePattern: String,
    projectionFn: SerializableFunction[T, R]
  ): PTransform[PBegin, PCollection[R]] =
    new PTransform[PBegin, PCollection[R]] {
      override def expand(input: PBegin): PCollection[R] = {
        input
          .apply(FileIO.`match`().filepattern(filePattern))
          .apply(FileIO.readMatches)
          .apply(readFiles(readSupportFactory, conf, projectionFn))
      }
    }

  def readFiles[T, R](
    readSupportFactory: ReadSupportFactory[T],
    conf: SerializableConfiguration,
    projectionFn: SerializableFunction[T, R]
  ): ParDo.SingleOutput[ReadableFile, R] = {
    val sdf = new ParquetReadFn[T, R](readSupportFactory, conf, projectionFn)
    ParDo.of(sdf)
  }
}
