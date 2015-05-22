package com.spotify.cloud.bigquery

import scala.language.implicitConversions

// A trait can be extended or mixed in
trait Implicits {

  implicit def makeRichTableRow(r: TableRow): RichTableRow = new RichTableRow(r)

}
