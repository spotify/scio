package com.spotify.cloud

import com.google.api.services.bigquery.model.{TableRow => GTableRow}

package object bigquery extends Implicits {

  object TableRow {
    def apply(fields: (String, _)*) = fields.foldLeft(new GTableRow())((r, kv) => r.set(kv._1, kv._2))
  }

  type TableRow = GTableRow

}
