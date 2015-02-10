package com.spotify.cloud.bigquery

// A trait can be extended or mixed in
trait Implicits {

  implicit def makeRichTableRow(r: TableRow): RichTableRow = new RichTableRow(r)

}
