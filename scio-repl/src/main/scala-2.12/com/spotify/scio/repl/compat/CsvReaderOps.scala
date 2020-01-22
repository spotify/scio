package com.spotify.scio.repl.compat

import kantan.csv.CsvReader

final class CsvReaderOps[A](val reader: CsvReader[A]) extends AnyVal {

  def iterator: Iterator[A] = reader.toIterator

}
