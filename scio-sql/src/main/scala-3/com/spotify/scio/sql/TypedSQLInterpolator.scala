package com.spotify.scio.sql

import com.spotify.scio.annotations.experimental

trait TypedSQLInterpolator {
  @experimental
  def tsql(ps: Any*): SQLBuilder = ???
}
