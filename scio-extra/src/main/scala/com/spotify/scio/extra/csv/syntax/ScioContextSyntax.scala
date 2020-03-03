package com.spotify.scio.extra.csv.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder
import com.spotify.scio.extra.csv.CsvIO
import com.spotify.scio.extra.csv.CsvIO.DefaultReadParams
import com.spotify.scio.values.SCollection
import kantan.csv.HeaderDecoder

trait ScioContextSyntax {
  implicit final class CsvScioContext(private val self: ScioContext) {
    @experimental
    def csvFile[T: HeaderDecoder: Coder](
      path: String,
      params: CsvIO.ReadParam = DefaultReadParams
    ): SCollection[T] =
      self.read(CsvIO.Read[T](path))(params)
  }
}
