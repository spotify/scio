package com.spotify.scio.parquet.read

import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.transforms.{Create, PTransform, ParDo, SerializableFunction}
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
          .apply(Create.ofProvider(StaticValueProvider.of(filePattern), StringUtf8Coder.of))
          .apply(FileIO.matchAll)
          .apply(FileIO.readMatches)
          .apply(readFiles(readSupportFactory, conf, projectionFn))
      }
    }

  def readFiles[T, R](
    readSupportFactory: ReadSupportFactory[T],
    conf: SerializableConfiguration,
    projectionFn: SerializableFunction[T, R]
  ): PTransform[PCollection[ReadableFile], PCollection[R]] =
    new PTransform[PCollection[ReadableFile], PCollection[R]] {
      override def expand(input: PCollection[ReadableFile]): PCollection[R] = {
        val sdf = new ParquetReadFn[T, R](readSupportFactory, conf, projectionFn)
        input.apply(ParDo.of(sdf))
      }
    }

}
